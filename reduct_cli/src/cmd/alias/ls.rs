// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::ConfigFile;
use crate::context::CliContext;
use crate::io::std::output;
use clap::Command;

pub(super) fn list_aliases(ctx: &CliContext) -> anyhow::Result<()> {
    let config_file = ConfigFile::load(ctx.config_path())?;
    let config = config_file.config();
    for (name, alias) in config.aliases.iter() {
        output!(ctx, "{}: {}", name, alias.url);
    }
    Ok(())
}

pub(super) fn ls_aliases_cmd() -> Command {
    Command::new("ls").about("List all aliases")
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::tests::context;

    use rstest::rstest;

    #[rstest]
    fn test_list_aliases(context: CliContext) {
        list_aliases(&context).unwrap();
        assert_eq!(
            context.stdout().history(),
            vec![
                "default: https://default.store/",
                "local: http://localhost:8383/"
            ]
        );
    }
}
