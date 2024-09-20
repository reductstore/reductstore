// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use log::trace;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TaskGroup {
    name: String,
    unique_locked: bool,
    use_count: i32,
    children: HashMap<String, TaskGroup>,
}

impl TaskGroup {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn children(&self) -> &HashMap<String, TaskGroup> {
        &self.children
    }

    pub fn unique_locked(&self) -> bool {
        self.unique_locked
    }

    pub fn use_count(&self) -> i32 {
        self.use_count
    }

    pub(super) fn new<T>(name: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            name: name.into(),
            unique_locked: false,
            use_count: 0,
            children: HashMap::new(),
        }
    }

    /// Lock a task group for a unique execution.
    pub fn lock(&mut self, path: &Vec<&str>, unique: bool) {
        let group = self.find_or_create(path);
        group.unique_locked = unique;
        group.use_count += 1;
    }

    /// Check if a task group is ready for an execution
    ///
    /// The method checks unique_lock and use_count for parent groups.
    pub(super) fn is_ready(&self, path: &Vec<&str>, unique: bool) -> bool {
        // Check all parent groups
        for i in 0..path.len() {
            let find_path = path[..i].to_vec();
            if let Some(group) = self.find(&find_path) {
                if group.unique_locked || (group.use_count > 0 && unique) {
                    trace!(
                        "Task group '{:?}' is locked for unique_lock: {}",
                        group,
                        unique
                    );
                    return false;
                }
            }
        }

        self.is_ready_current(path, unique)
    }

    /// Check only the current group if it is ready for an execution
    pub(super) fn is_ready_current(&self, path: &Vec<&str>, unique: bool) -> bool {
        let group = self.find(path);
        if let Some(group) = group {
            if group.unique_locked || (group.use_count > 0 && unique) {
                trace!(
                    "Task group '{:?}' is locked for unique_lock: {}",
                    group,
                    unique
                );
                return false;
            }

            // Check children
            for child in group.children.values() {
                if !child.is_ready_current(&vec![], unique) {
                    return false;
                }
            }
        }
        true
    }

    pub(super) fn unlock(&mut self, path: &Vec<&str>, unique: bool) {
        let mut path = path.to_vec();
        let mut group = self.find_mut(&path).unwrap();
        if unique {
            group.unique_locked = false;
        }

        group.use_count -= 1;
        if group.use_count < 0 {
            panic!("Task group use count is negative");
        }

        // Remove unused groups up to the root
        while path.len() > 0 && group.use_count == 0 && group.children.is_empty() {
            let name = path.last().unwrap().to_string();
            path = path[..path.len() - 1].to_vec();
            let mut parent = self
                .find_mut(&path)
                .expect(format!("Parent group not found: {}", path.join("/")).as_str());
            parent.children.remove(&name);
            group = parent;
        }
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

    pub(super) fn find(&self, path: &Vec<&str>) -> Option<&TaskGroup> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};

    #[rstest]
    fn test_create_delete_groups(mut group: TaskGroup) {
        group.lock(&vec!["a", "b"], false);

        assert!(group.find(&vec!["a"]).is_some());
        assert!(group.find(&vec!["a", "b"]).is_some());

        group.unlock(&vec!["a", "b"], false);

        assert!(group.find(&vec!["a", "b"]).is_none());
        assert!(group.find(&vec!["a"]).is_none());
    }

    #[rstest]
    fn test_use_count(mut group: TaskGroup) {
        group.lock(&vec!["a", "b1"], false);
        group.lock(&vec!["a", "b2"], false);

        assert_eq!(group.use_count(), 0);
        assert_eq!(group.find(&vec!["a"]).unwrap().use_count(), 0);
        assert_eq!(group.find(&vec!["a", "b1"]).unwrap().use_count(), 1);
        assert_eq!(group.find(&vec!["a", "b1"]).unwrap().use_count(), 1);

        group.unlock(&vec!["a", "b1"], false);
        assert_eq!(group.use_count(), 0);
        assert_eq!(group.find(&vec!["a"]).unwrap().use_count(), 0);
        assert_eq!(group.find(&vec!["a", "b1"]), None);
        assert_eq!(group.find(&vec!["a", "b2"]).unwrap().use_count(), 1);

        group.unlock(&vec!["a", "b2"], false);
        assert_eq!(group.use_count(), 0);
        assert_eq!(group.find(&vec!["a"]), None);
    }

    #[rstest]
    fn test_lock_parent(mut group: TaskGroup) {
        group.lock(&vec!["a", "b"], false);
        group.lock(&vec!["a"], false);

        assert_eq!(group.use_count(), 0);
        assert_eq!(group.find(&vec!["a"]).unwrap().use_count(), 1);
        assert_eq!(group.find(&vec!["a", "b"]).unwrap().use_count(), 1);
    }

    #[rstest]
    fn test_shared_lock(mut group: TaskGroup) {
        group.lock(&vec!["a", "b"], false);

        assert!(group.is_ready(&vec!["a", "b"], false));
        assert!(group.is_ready(&vec!["a"], false));
        assert!(group.is_ready(&vec![], false));

        assert!(!group.is_ready(&vec!["a", "b"], true));
        assert!(!group.is_ready(&vec!["a"], true));
        assert!(!group.is_ready(&vec![], true));

        group.unlock(&vec!["a", "b"], false);

        assert!(group.is_ready(&vec![], true));
        assert!(group.is_ready(&vec!["a"], true));
        assert!(group.is_ready(&vec!["a", "b"], true));
    }

    #[rstest]
    fn test_shared_lock_parent(mut group: TaskGroup) {
        group.lock(&vec!["a"], false);

        assert!(group.is_ready(&vec!["a", "b"], false));
        assert!(group.is_ready(&vec!["a"], false));
        assert!(group.is_ready(&vec![], false));

        assert!(!group.is_ready(&vec!["a", "b"], true));
        assert!(!group.is_ready(&vec!["a"], true));
        assert!(!group.is_ready(&vec![], true));

        assert!(
            group.is_ready_current(&vec!["a", "b"], true),
            "children aren't locked"
        );
        assert!(!group.is_ready_current(&vec!["a"], true));
        assert!(!group.is_ready_current(&vec![], true));

        group.unlock(&vec!["a"], false);

        assert!(group.is_ready(&vec![], true));
        assert!(group.is_ready(&vec!["a"], true));
        assert!(group.is_ready(&vec!["a", "b"], true));
    }

    #[rstest]
    fn test_unique_lock(mut group: TaskGroup) {
        group.lock(&vec!["a", "b"], true);

        assert!(!group.is_ready(&vec!["a", "b"], false));
        assert!(!group.is_ready(&vec!["a"], false));
        assert!(!group.is_ready(&vec![], false));

        assert!(!group.is_ready(&vec!["a", "b"], true));
        assert!(!group.is_ready(&vec!["a"], true));
        assert!(!group.is_ready(&vec![], true));

        group.unlock(&vec!["a", "b"], true);

        assert!(group.is_ready(&vec![], false));
        assert!(group.is_ready(&vec!["a"], false));
        assert!(group.is_ready(&vec!["a", "b"], false));

        assert!(group.is_ready(&vec![], true));
        assert!(group.is_ready(&vec!["a"], true));
        assert!(group.is_ready(&vec!["a", "b"], true));
    }

    #[rstest]
    fn test_unique_lock_parent(mut group: TaskGroup) {
        group.lock(&vec!["a"], true);

        assert!(!group.is_ready(&vec!["a", "b"], false));
        assert!(!group.is_ready(&vec!["a"], false));
        assert!(!group.is_ready(&vec![], false));

        assert!(!group.is_ready(&vec!["a", "b"], true));
        assert!(!group.is_ready(&vec!["a"], true));
        assert!(!group.is_ready(&vec![], true));

        assert!(
            group.is_ready_current(&vec!["a", "b"], true),
            "children aren't locked"
        );
        assert!(!group.is_ready_current(&vec!["a"], true));
        assert!(!group.is_ready_current(&vec![], true));

        group.unlock(&vec!["a"], true);

        assert!(group.is_ready(&vec![], false));
        assert!(group.is_ready(&vec!["a"], false));
        assert!(group.is_ready(&vec!["a", "b"], false));

        assert!(group.is_ready(&vec![], true));
        assert!(group.is_ready(&vec!["a"], true));
        assert!(group.is_ready(&vec!["a", "b"], true));
    }

    #[fixture]
    fn group() -> TaskGroup {
        TaskGroup::new("root")
    }
}
