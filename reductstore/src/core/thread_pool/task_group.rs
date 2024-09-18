// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use log::trace;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(super) struct TaskGroup {
    name: String,
    unique_lock: bool,
    use_count: i32,
    children: HashMap<String, TaskGroup>,
}

impl TaskGroup {
    pub fn new(name: String) -> Self {
        Self {
            name,
            unique_lock: false,
            use_count: 0,
            children: HashMap::new(),
        }
    }

    /// Lock a task group for a unique execution.
    pub fn lock(&mut self, path: &Vec<&str>, unique: bool) {
        let group = self.find_or_create(path);
        group.unique_lock = unique;
        group.use_count += 1;
    }

    /// Check if a task group is ready for an execution
    ///
    /// The method checks unique_lock and use_count for parent groups.
    pub fn is_ready(&self, path: &Vec<&str>, unique: bool) -> bool {
        for (i, name) in path.iter().enumerate() {
            if let Some(group) = self.find(&path[..i].to_vec()) {
                if group.unique_lock || (group.use_count > 0 && unique) {
                    trace!(
                        "Task group {} is locked: unique_lock: {}, use_count: {}",
                        path[..i].join("/"),
                        group.unique_lock,
                        group.use_count
                    );
                    return false;
                }
            }
        }
        true
    }

    /// Check only the current group if it is ready for an execution
    pub fn is_ready_current(&self, path: &Vec<&str>, unique: bool) -> bool {
        let group = self.find(path);
        if let Some(group) = group {
            if group.unique_lock || (group.use_count > 0 && unique) {
                trace!(
                    "Task child group {} is locked: unique_lock: {}, use_count: {}",
                    path.join("/"),
                    group.unique_lock,
                    group.use_count
                );
                return false;
            }
        }
        true
    }

    pub fn unlock(&mut self, path: &Vec<&str>, unique: bool) {
        let mut group = self.find_mut(path).unwrap();
        if unique {
            group.unique_lock = false;
        }

        group.use_count -= 1;
        if group.use_count < 0 {
            panic!("Task group use count is negative");
        }

        // remove empty groups
        group.children.retain(|_, group| {
            if group.use_count == 0 && !group.unique_lock && group.children.is_empty() {
                return false;
            }
            true
        });
    }

    fn find_or_create(&mut self, path: &Vec<&str>) -> &mut TaskGroup {
        if path.is_empty() {
            return self;
        }

        let name = path[0];
        let path = path[1..].to_vec();

        match self.children.entry(name.to_string()) {
            Entry::Occupied(entry) => entry.into_mut().find_or_create(&path),
            Entry::Vacant(entry) => entry
                .insert(TaskGroup::new(name.to_string()))
                .find_or_create(&path),
        }
    }

    fn find(&self, path: &Vec<&str>) -> Option<&TaskGroup> {
        if path.is_empty() {
            return Some(self);
        }

        let name = path[0];
        let path = path[1..].to_vec();

        self.children.get(name).and_then(|group| group.find(&path))
    }

    fn find_mut(&mut self, path: &Vec<&str>) -> Option<&mut TaskGroup> {
        if path.is_empty() {
            return Some(self);
        }

        let name = path[0];
        let path = path[1..].to_vec();

        self.children
            .get_mut(name)
            .and_then(|group| group.find_mut(&path))
    }
}
