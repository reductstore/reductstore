// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub(crate) mod alias;
pub(crate) mod bucket;
pub(crate) mod server;

const ALIAS_OR_URL_HELP: &str =
    "Alias or URL (e.g. http://token@localhost:8383) of the ReductStore instance to use";
const BUCKET_PATH_HELP: &str = "Path to the bucket to use (e.g. SERVER_ALIAS/BUCKET_NAME or http://token@localhost:8383/BUCKET_NAME)";
