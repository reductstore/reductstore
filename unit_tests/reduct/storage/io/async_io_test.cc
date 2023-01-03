// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include <catch2/catch.hpp>

#include "reduct/config.h"
#include "reduct/helpers.h"
#include "reduct/storage/entry.h"

using reduct::core::Error;
using reduct::core::Time;
using reduct::storage::IEntry;

using reduct::ReadOne;
using reduct::WriteOne;
using reduct::async::IAsyncReader;

using std::chrono::seconds;

std::string_view kName = "entry_1";

static auto MakeDefaultOptions() {
  return IEntry::Options{
      .max_block_size = 1000,
      .max_block_records = 100,
  };
}

static const auto kTimestamp = Time();

TEST_CASE("AsyncWriter should provide async writing in the same block") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  auto [writer_1, err_1] = entry->BeginWrite(kTimestamp, 10);
  REQUIRE(err_1 == Error::kOk);

  auto [writer_2, err_2] = entry->BeginWrite(kTimestamp + seconds(1), 10);
  REQUIRE(err_2 == Error::kOk);

  REQUIRE(writer_1->Write("aaaaa", false) == Error::kOk);
  REQUIRE(writer_2->Write("bbbbb", false) == Error::kOk);
  REQUIRE(writer_1->Write("ccccc") == Error::kOk);
  REQUIRE(writer_2->Write("ddddd") == Error::kOk);

  REQUIRE(ReadOne(*entry, kTimestamp).result == "aaaaaccccc");
  REQUIRE(ReadOne(*entry, kTimestamp + seconds(1)).result == "bbbbbddddd");
}

TEST_CASE("AsyncWriter should check size") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  SECTION("too long") {
    auto [writer, err] = entry->BeginWrite(kTimestamp, 10);
    REQUIRE(writer->Write("123456", false) == Error::kOk);
    REQUIRE(writer->Write("123456") == Error::BadRequest("Content is bigger than in content-length"));
  }

  SECTION("too short") {
    auto [writer, err] = entry->BeginWrite(kTimestamp, 10);
    REQUIRE(writer->Write("123456", false) == Error::kOk);
    REQUIRE(writer->Write("12") == Error::BadRequest("Content is smaller than in content-length"));
  }
}

TEST_CASE("AsyncWriter should mark finished records") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  auto [writer, err] = entry->BeginWrite(kTimestamp, 10);
  REQUIRE(writer->Write("123", false) == Error::kOk);

  REQUIRE(ReadOne(*entry, kTimestamp) == Error::TooEarly("Record is still being written"));

  REQUIRE(writer->Write("456789012") != Error::kOk);
  REQUIRE(ReadOne(*entry, kTimestamp) == Error::InternalError("Record is broken"));
}

TEST_CASE("AsyncReader should read a big file in two chunks") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  const auto size = reduct::kDefaultMaxReadChunk * 2 - 1;
  REQUIRE(WriteOne(*entry, std::string(size, 'x'), kTimestamp) == Error::kOk);

  auto [reader, err] = entry->BeginRead(kTimestamp);
  REQUIRE(err == Error::kOk);
  REQUIRE(reader->size() == size);
  REQUIRE(reader->Read().result == IAsyncReader::DataChunk{std::string(reduct::kDefaultMaxReadChunk, 'x'), false});
  REQUIRE(reader->Read().result == IAsyncReader::DataChunk{std::string(reduct::kDefaultMaxReadChunk - 1, 'x'), true});
}

TEST_CASE("AsyncReader should not spoil data") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  const auto size = reduct::kDefaultMaxReadChunk - 1;
  std::string blob(size, 'a');
  for (int i = 0; i < size; ++i) {
    blob[i] = static_cast<char>(rand() % 127);  // NOLINT
  }

  REQUIRE(WriteOne(*entry, blob, kTimestamp) == Error::kOk);

  auto [reader, err] = entry->BeginRead(kTimestamp);
  REQUIRE(err == Error::kOk);
  REQUIRE(reader->Read().result == IAsyncReader::DataChunk{blob, true});
}

TEST_CASE("AsyncReader should read labels in the begging of reading") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);
  const IEntry::LabelMap labels{{"label1", "value1"}, {"label2", "value2"}};
  REQUIRE(WriteOne(*entry, "some_data", kTimestamp, labels) == Error::kOk);

  auto [reader, err] = entry->BeginRead(kTimestamp);
  REQUIRE(err == Error::kOk);
  REQUIRE(reader->labels() == labels);
}
