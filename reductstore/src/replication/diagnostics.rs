// Copyright 2024 ReductStore
// Licensed under the Business Source License 1.1

use reduct_base::error::{IntEnum, ReductError};
use reduct_base::msg::diagnostics::{DiagnosticsError, DiagnosticsItem};
use std::collections::hash_map::Entry;

use std::time::{Duration, Instant};

pub(super) struct DiagnosticsCounter {
    frames: Vec<DiagnosticsItem>,
    frame_interval: Duration,
    frame_count: u32,
    frame_last: Instant,
}

const DEFAULT_FRAME_COUNT: u32 = 60;

impl DiagnosticsCounter {
    pub(super) fn new(count_interval: Duration) -> Self {
        Self {
            frames: vec![DiagnosticsItem::default()],
            frame_interval: count_interval / DEFAULT_FRAME_COUNT,
            frame_count: DEFAULT_FRAME_COUNT,
            frame_last: Instant::now(),
        }
    }

    pub(super) fn count(&mut self, result: Result<(), ReductError>) {
        self.check_and_create_new_frame();
        let frame = self.frames.last_mut().unwrap();
        // count the result
        match result {
            Ok(_) => frame.ok += 1,
            Err(err) => {
                frame.errored += 1;

                // count errors by type
                match frame.errors.entry(err.status.int_value()) {
                    Entry::Occupied(mut entry) => {
                        let entry = entry.get_mut();
                        entry.count += 1;
                        entry.last_message = err.message;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(DiagnosticsError {
                            count: 1,
                            last_message: err.message,
                        });
                    }
                }
            }
        }
    }

    pub(super) fn diagnostics(&self) -> DiagnosticsItem {
        let mut diagnostics = self
            .frames
            .iter()
            .fold(DiagnosticsItem::default(), |acc, frame| DiagnosticsItem {
                ok: acc.ok + frame.ok,
                errored: acc.errored + frame.errored,
                errors: frame
                    .errors
                    .iter()
                    .fold(acc.errors.clone(), |mut acc, (code, err)| {
                        match acc.entry(*code) {
                            Entry::Occupied(mut entry) => {
                                let entry = entry.get_mut();
                                entry.count += err.count;
                                entry.last_message = err.last_message.clone();
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(err.clone());
                            }
                        }
                        acc
                    }),
            });

        // calculate the average for the last DEFAULT_FRAME_COUNT frames
        let k = self.frames.len() as f32 / self.frame_count as f32;
        diagnostics.ok = (diagnostics.ok as f32 / k).round() as u64;
        diagnostics.errored = (diagnostics.errored as f32 / k).round() as u64;
        diagnostics
    }

    fn check_and_create_new_frame(&mut self) {
        let delta = self.frame_last.elapsed().as_micros() / self.frame_interval.as_micros();
        for _ in 0..delta {
            self.frame_last = Instant::now();
            self.frames.push(DiagnosticsItem::default());
            if self.frames.len() > self.frame_count.try_into().unwrap() {
                self.frames.remove(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::error::ReductError;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

    const FRAME_INTERVAL_MS: u64 = 20;

    #[rstest]

    fn test_diagnostics_counter_ok(_counter: DiagnosticsCounter) {
        let mut counter = DiagnosticsCounter::new(Duration::from_millis(
            DEFAULT_FRAME_COUNT as u64 * FRAME_INTERVAL_MS,
        ));
        counter.count(Ok(()));

        assert_eq!(
            counter.diagnostics().ok,
            60,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );
        assert_eq!(counter.diagnostics().errored, 0);

        wait_for_next_frame();
        counter.count(Ok(()));
        assert_eq!(
            counter.diagnostics().ok,
            60,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );

        wait_for_next_frame();
        counter.count(Ok(()));
        counter.count(Ok(()));
        assert_eq!(
            counter.diagnostics().ok,
            80,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );

        for _ in 0..DEFAULT_FRAME_COUNT {
            wait_for_next_frame();
            counter.count(Ok(()));
        }

        assert_eq!(
            counter.diagnostics().ok,
            60,
            "should calculate for DEFAULT_FRAME_COUNT intervals"
        );
    }

    fn wait_for_next_frame() {
        sleep(Duration::from_millis(FRAME_INTERVAL_MS));
    }

    #[rstest]
    fn test_diagnostics_counter_err(_counter: DiagnosticsCounter) {
        let mut counter = DiagnosticsCounter::new(Duration::from_millis(
            DEFAULT_FRAME_COUNT as u64 * FRAME_INTERVAL_MS,
        ));
        counter.count(Err(ReductError::internal_server_error("test")));

        assert_eq!(
            counter.diagnostics().errored,
            60,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );
        assert_eq!(counter.diagnostics().ok, 0);

        wait_for_next_frame();
        counter.count(Err(ReductError::internal_server_error("test")));
        assert_eq!(
            counter.diagnostics().errored,
            60,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );

        wait_for_next_frame();
        counter.count(Err(ReductError::internal_server_error("test")));
        counter.count(Err(ReductError::internal_server_error("test")));
        assert_eq!(
            counter.diagnostics().errored,
            80,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );

        for _ in 0..DEFAULT_FRAME_COUNT {
            wait_for_next_frame();
            counter.count(Err(ReductError::internal_server_error("test")));
        }

        assert_eq!(
            counter.diagnostics().errored,
            60,
            "should calculate for DEFAULT_FRAME_COUNT intervals"
        );
    }

    #[rstest]
    fn test_gaps_in_frames(mut counter: DiagnosticsCounter) {
        counter.count(Err(ReductError::internal_server_error("test")));
        counter.count(Ok(()));

        sleep(Duration::from_millis(FRAME_INTERVAL_MS * 2));

        counter.count(Err(ReductError::internal_server_error("test")));
        counter.count(Ok(()));

        assert_eq!(
            counter.diagnostics().errored,
            40,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );
        assert_eq!(
            counter.diagnostics().ok,
            40,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );

        for _ in 0..DEFAULT_FRAME_COUNT / 2 {
            sleep(Duration::from_millis(FRAME_INTERVAL_MS * 2));
            counter.count(Ok(()));
            counter.count(Err(ReductError::internal_server_error("test")));
        }

        assert_eq!(
            counter.diagnostics().errored,
            30,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );
        assert_eq!(
            counter.diagnostics().ok,
            30,
            "should approximate for DEFAULT_FRAME_COUNT intervals"
        );
    }

    #[rstest]
    fn test_error_map_same_type(mut counter: DiagnosticsCounter) {
        counter.count(Err(ReductError::internal_server_error("test")));
        counter.count(Err(ReductError::internal_server_error("test-1")));

        assert_eq!(
            counter.diagnostics().errors,
            HashMap::from_iter(vec![(
                500,
                DiagnosticsError {
                    count: 2,
                    last_message: "test-1".to_string(),
                }
            ),]),
            "should count errors of the same type"
        );
    }

    #[rstest]
    fn test_error_map_different_type(mut counter: DiagnosticsCounter) {
        counter.count(Err(ReductError::internal_server_error("test")));
        counter.count(Err(ReductError::bad_request("test-1")));

        assert_eq!(
            counter.diagnostics().errors,
            HashMap::from_iter(vec![
                (
                    500,
                    DiagnosticsError {
                        count: 1,
                        last_message: "test".to_string(),
                    }
                ),
                (
                    400,
                    DiagnosticsError {
                        count: 1,
                        last_message: "test-1".to_string(),
                    }
                )
            ]),
            "should count errors of the same type"
        );
    }

    #[rstest]
    fn test_error_map_frames(mut counter: DiagnosticsCounter) {
        counter.count(Err(ReductError::internal_server_error("test")));
        counter.count(Err(ReductError::bad_request("test-1")));

        assert_eq!(
            counter.diagnostics().errors,
            HashMap::from_iter(vec![
                (
                    500,
                    DiagnosticsError {
                        count: 1,
                        last_message: "test".to_string(),
                    }
                ),
                (
                    400,
                    DiagnosticsError {
                        count: 1,
                        last_message: "test-1".to_string(),
                    }
                )
            ]),
            "should count errors of the same type"
        );

        for i in 0..DEFAULT_FRAME_COUNT / 2 {
            sleep(Duration::from_millis(FRAME_INTERVAL_MS * 2));
            counter.count(Err(ReductError::internal_server_error(&format!(
                "test-{}",
                i
            ))));
            counter.count(Err(ReductError::bad_request(&format!("test-{}", i))));
        }

        assert_eq!(
            counter.diagnostics().errors,
            HashMap::from_iter(vec![
                (
                    500,
                    DiagnosticsError {
                        count: 30,
                        last_message: "test-29".to_string(),
                    }
                ),
                (
                    400,
                    DiagnosticsError {
                        count: 30,
                        last_message: "test-29".to_string(),
                    }
                )
            ]),
            "should count errors of the same type"
        );
    }

    #[fixture]
    fn counter() -> DiagnosticsCounter {
        DiagnosticsCounter::new(Duration::from_millis(
            DEFAULT_FRAME_COUNT as u64 * FRAME_INTERVAL_MS,
        ))
    }
}
