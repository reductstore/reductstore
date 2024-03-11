// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub(crate) trait Output {
    fn print(&self, message: &str);

    #[cfg(test)]
    fn history(&self) -> Vec<String>;
}

pub(crate) struct StdOutput;

impl StdOutput {
    pub(crate) fn new() -> Self {
        StdOutput {}
    }
}

impl Output for StdOutput {
    fn print(&self, message: &str) {
        println!("{}", message);
    }

    #[cfg(test)]
    fn history(&self) -> Vec<String> {
        Vec::new()
    }
}

macro_rules! output {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.stdout().print(&format!($($arg)*));
    };
}

pub(crate) use output;

pub(crate) trait Input {
    fn read(&self) -> Result<String, anyhow::Error>;

    #[cfg(test)]
    fn emulate(&self, input: Vec<&'static str>);
}

pub(crate) struct StdInput;

impl StdInput {
    pub(crate) fn new() -> Self {
        StdInput {}
    }
}

impl Input for StdInput {
    fn read(&self) -> Result<String, anyhow::Error> {
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        Ok(input.trim().to_string())
    }

    #[cfg(test)]
    fn emulate(&self, _input: Vec<&'static str>) {}
}

macro_rules! input {
    ($ctx:expr) => {
        $ctx.stdin().read()
    };
}
