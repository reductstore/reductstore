// Copyright 2022 Alexey Timin
#include <catch2/catch.hpp>

#include "reduct/config.h"
#include "reduct/helpers.h"
#include "reduct/storage/entry.h"

using reduct::core::Error;
using reduct::storage::IEntry;

using reduct::ReadOne;
using reduct::WriteOne;
using reduct::async::IAsyncReader;

using std::chrono::seconds;

static auto MakeDefaultOptions() {
  return IEntry::Options{
      .name = "entry_1",
      .path = BuildTmpDirectory(),
      .max_block_size = 1000,
  };
}

static const auto kTimestamp = IEntry::Time();

TEST_CASE("AsyncWriter should provide async writing in the same block") {
  auto entry = IEntry::Build(MakeDefaultOptions());
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
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  auto [writer, err] = entry->BeginWrite(kTimestamp, 10);
  REQUIRE(writer->Write("123456") == Error::kOk);
  REQUIRE(writer->Write("123456") == Error{.code = 413, .message = "Content is bigger than in content-length"});
}

TEST_CASE("AsyncWriter should mark finished records") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  auto [writer, err] = entry->BeginWrite(kTimestamp, 10);
  REQUIRE(writer->Write("123", false) == Error::kOk);

  REQUIRE(ReadOne(*entry, kTimestamp) == Error{.code = 425, .message = "Record is still being written"});

  REQUIRE(writer->Write("456789012") != Error::kOk);
  REQUIRE(ReadOne(*entry, kTimestamp) == Error{.code = 500, .message = "Record is broken"});
}

TEST_CASE("AsyncReader should read a big file in two chunks") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  const auto size = reduct::kDefaultMaxReadChunk * 2 - 1;
  REQUIRE(WriteOne(*entry, std::string(size, 'x'), kTimestamp) == Error::kOk);

  auto [reader, err] = entry->BeginRead(kTimestamp);
  REQUIRE(err == Error::kOk);
  REQUIRE(reader->size() == size);
  REQUIRE(reader->Read().result == IAsyncReader::DataChunk{std::string(reduct::kDefaultMaxReadChunk, 'x'), false});
  REQUIRE(reader->Read().result == IAsyncReader::DataChunk{std::string(reduct::kDefaultMaxReadChunk - 1, 'x'), true});
}
