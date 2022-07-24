// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <catch2/catch.hpp>
#include <google/protobuf/util/time_util.h>

#include <filesystem>

#include "reduct/helpers.h"

using reduct::ReadOne;
using reduct::WriteOne;
using reduct::core::Error;
using reduct::storage::IEntry;

using google::protobuf::util::TimeUtil;

using std::chrono::seconds;
namespace fs = std::filesystem;

const auto kName = "entry_1";

static auto MakeDefaultOptions() {
  return IEntry::Options{
      .max_block_size = 100,
      .max_block_records = 1024,
  };
}

static const auto kTimestamp = IEntry::Time() + std::chrono::microseconds(10'100'200);  // to check um precision

auto ToMicroseconds(IEntry::Time tp) {
  return std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()).count();
}

auto GetBlockSize(std::string_view name, fs::path path, IEntry::Options options, IEntry::Time begin_ts) {
  return fs::file_size(
      path / name /
      fmt::format("{}.blk",
                  std::chrono::duration_cast<std::chrono::microseconds>(begin_ts.time_since_epoch()).count()));
}

TEST_CASE("storage::Entry should record data to a block", "[entry]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(WriteOne(*entry, "some_data", kTimestamp) == Error::kOk);

  SECTION("one record") {
    REQUIRE(ReadOne(*entry, kTimestamp).result == "some_data");
    const auto info = entry->GetInfo();
    REQUIRE(info.name() == "entry_1");
    REQUIRE(info.size() == 9);
    REQUIRE(info.record_count() == 1);
    REQUIRE(info.block_count() == 1);
    REQUIRE(info.latest_record() == ToMicroseconds(kTimestamp));
    REQUIRE(info.oldest_record() == ToMicroseconds(kTimestamp));
  }

  SECTION("few records") {
    REQUIRE(WriteOne(*entry, "other_data1", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "other_data2", kTimestamp + seconds(10)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "other_data3", kTimestamp + seconds(15)) == Error::kOk);

    const auto info = entry->GetInfo();
    REQUIRE(info.name() == "entry_1");
    REQUIRE(info.size() == 42);
    REQUIRE(info.record_count() == 4);
    REQUIRE(info.block_count() == 1);
    REQUIRE(info.oldest_record() == ToMicroseconds(kTimestamp));
    REQUIRE(info.latest_record() == ToMicroseconds(kTimestamp + seconds(15)));

    REQUIRE(ReadOne(*entry, kTimestamp).result == "some_data");
    REQUIRE(ReadOne(*entry, kTimestamp + seconds(15)).result == "other_data3");
  }

  SECTION("error 404 if request out of  interval") {
    REQUIRE(ReadOne(*entry, kTimestamp - seconds(10)) ==
            Error{.code = 404, .message = "No records for this timestamp"});
  }
}

TEST_CASE("storage::Entry should create a new block if the current > max_block_size", "[entry]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  auto big_data = std::string(MakeDefaultOptions().max_block_size + 1, 'c');
  REQUIRE(WriteOne(*entry, big_data, kTimestamp) == Error::kOk);

  SECTION("one record") {
    REQUIRE(ReadOne(*entry, kTimestamp).result == big_data);

    const auto info = entry->GetInfo();
    REQUIRE(info.name() == "entry_1");
    REQUIRE(info.size() == 101);
    REQUIRE(info.record_count() == 1);
    REQUIRE(info.block_count() == 1);
    REQUIRE(info.latest_record() == ToMicroseconds(kTimestamp));
    REQUIRE(info.oldest_record() == ToMicroseconds(kTimestamp));
  }

  SECTION("two records in different blocks") {
    REQUIRE(WriteOne(*entry, "other_data1", kTimestamp + seconds(5)) == Error::kOk);

    const auto info = entry->GetInfo();
    REQUIRE(info.size() == 112);
    REQUIRE(info.record_count() == 2);
    REQUIRE(info.block_count() == 2);
    REQUIRE(info.oldest_record() == ToMicroseconds(kTimestamp));
    REQUIRE(info.latest_record() == ToMicroseconds(kTimestamp + seconds(5)));

    REQUIRE(ReadOne(*entry, kTimestamp + seconds(5)).result == "other_data1");
  }
}

TEST_CASE("storage::Entry should resize finished block", "[entry][block]") {
  const auto options = MakeDefaultOptions();
  const auto path = BuildTmpDirectory();
  auto entry = IEntry::Build(kName, path, options);
  REQUIRE(entry);

  auto big_data = std::string(options.max_block_size - 10, 'c');
  REQUIRE(WriteOne(*entry, big_data, kTimestamp) == Error::kOk);
  REQUIRE(WriteOne(*entry, big_data, kTimestamp + seconds(1)) == Error::kOk);

  REQUIRE(GetBlockSize(kName, path, options, kTimestamp) == big_data.size());
  REQUIRE(GetBlockSize(kName, path, options, kTimestamp + seconds(1)) == options.max_block_size);
}

TEST_CASE("storage::Entry start a new block if it has more records than max_block_records", "[entry][block]") {
  auto options = MakeDefaultOptions();
  options.max_block_records = 2;
  const auto path = BuildTmpDirectory();
  auto entry = IEntry::Build(kName, path, options);

  REQUIRE(entry);

  REQUIRE(WriteOne(*entry, "data", kTimestamp) == Error::kOk);
  REQUIRE(WriteOne(*entry, "data", kTimestamp + seconds(1)) == Error::kOk);
  REQUIRE(WriteOne(*entry, "data", kTimestamp + seconds(2)) == Error::kOk);

  REQUIRE(GetBlockSize(kName, path, options, kTimestamp) == 8);
  REQUIRE(GetBlockSize(kName, path, options, kTimestamp + seconds(2)) == options.max_block_size);
}

TEST_CASE("storage::Entry should create block with size of record it  is bigger than max_block_size",
          "[entry][block]") {
  const auto options = MakeDefaultOptions();
  const auto path = BuildTmpDirectory();
  auto entry = IEntry::Build(kName, path, options);
  REQUIRE(entry);

  auto big_data = std::string(options.max_block_size + 10, 'c');
  REQUIRE(WriteOne(*entry, big_data, kTimestamp) == Error::kOk);

  REQUIRE(GetBlockSize(kName, path, options, kTimestamp) == big_data.size());
}

TEST_CASE("storage::Entry should write data for random kTimestamp", "[entry]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  auto big_blob = std::string(100, 'c');
  REQUIRE(entry->BeginWrite(kTimestamp, big_blob.size()).result->Write(big_blob) == Error::kOk);

  SECTION("a record older than first in entry") {
    REQUIRE(WriteOne(*entry, "belated_data", kTimestamp - seconds(5)) == Error::kOk);
    REQUIRE(ReadOne(*entry, kTimestamp - seconds(5)).result == "belated_data");
  }

  SECTION("a belated record") {
    REQUIRE(entry->GetInfo().block_count() == 1);
    REQUIRE(WriteOne(*entry, "latest_data", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "latest_data", kTimestamp + seconds(15)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "belated_data", kTimestamp + seconds(10)) == Error::kOk);

    REQUIRE(ReadOne(*entry, kTimestamp + seconds(10)).result == "belated_data");
  }
}

TEST_CASE("storage::Entry should restore itself from folder", "[entry]") {
  const auto options = MakeDefaultOptions();
  const auto path = BuildTmpDirectory();
  auto entry = IEntry::Build(kName, path, options);

  REQUIRE(entry);
  REQUIRE(WriteOne(*entry, "some_data", kTimestamp) == Error::kOk);
  entry = IEntry::Build(kName, path, options);
  REQUIRE(entry->GetOptions() == options);

  const auto info = entry->GetInfo();
  REQUIRE(info.size() == 9);
  REQUIRE(info.record_count() == 1);
  REQUIRE(info.block_count() == 1);
  REQUIRE(info.latest_record() == ToMicroseconds(kTimestamp));
  REQUIRE(info.oldest_record() == ToMicroseconds(kTimestamp));

  SECTION("should work ok after restoring") {
    REQUIRE(WriteOne(*entry, "next_data", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(ReadOne(*entry, kTimestamp + seconds(5)).result == "next_data");
  }
}

TEST_CASE("storage::Entry should read from empty entry with 404", "[entry]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());

  REQUIRE(entry);
  REQUIRE(ReadOne(*entry, IEntry::Time()).error.code == 404);
}

TEST_CASE("storage::Entry should remove last block", "[entry]") {
  const auto path = BuildTmpDirectory();
  auto entry = IEntry::Build(kName, path, MakeDefaultOptions());
  REQUIRE(entry);

  const std::string blob(entry->GetOptions().max_block_size + 1, 'x');
  REQUIRE(WriteOne(*entry, blob, kTimestamp) == Error::kOk);
  REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(1)) == Error::kOk);
  REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(2)) == Error::kOk);
  REQUIRE(entry->GetInfo().block_count() == 3);

  SECTION("remove one block") {
    REQUIRE(entry->RemoveOldestBlock() == Error::kOk);
    REQUIRE(entry->GetInfo().block_count() == 2);
    REQUIRE(ReadOne(*entry, kTimestamp).error.code == 404);

    SECTION("write should be ok") {
      REQUIRE(WriteOne(*entry, "some_data", kTimestamp) == Error::kOk);
      REQUIRE(ReadOne(*entry, kTimestamp).error == Error::kOk);
    }

    SECTION("remove two blocks") {
      REQUIRE(entry->RemoveOldestBlock() == Error::kOk);
      REQUIRE(entry->GetInfo().block_count() == 1);
      REQUIRE(ReadOne(*entry, kTimestamp + seconds(1)).error.code == 404);
    }

    SECTION("recovery") {
      auto info = entry->GetInfo();
      entry = IEntry::Build(kName, path, entry->GetOptions());

      REQUIRE(entry);
      REQUIRE(entry->GetInfo() == info);
    }
  }
}

TEST_CASE("storage::Entry should list records for time interval", "[entry]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  SECTION("empty record") {
    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(1));
    REQUIRE(err == Error{.code = 404, .message = "No records in the entry"});
    REQUIRE(records.empty());
  }

  SECTION("some records in few blocks") {
    const std::string blob(entry->GetOptions().max_block_size, 'x');
    REQUIRE(WriteOne(*entry, blob, kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(2)) == Error::kOk);

    SECTION("without overlap") {
      auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

      REQUIRE(err == Error::kOk);
      REQUIRE(records.size() == 2);
      REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 100});
      REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 100});
    }

    SECTION("with overlap") {
      auto [records, err] = entry->List(kTimestamp - seconds(1), kTimestamp + seconds(4));
      REQUIRE(err == Error::kOk);
      REQUIRE(records.size() == 3);
    }
  }

  SECTION("some records in one block") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 4});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 4});
  }

  SECTION("list should be sorted") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 4});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 4});
  }

  SECTION("extreme cases") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    SECTION("request data before first record") {
      auto [records, err] = entry->List(kTimestamp - seconds(2), kTimestamp - seconds(1));
      REQUIRE(err == Error{.code = 404, .message = "No records for time interval"});
      REQUIRE(records.empty());
    }

    SECTION("request data after last record") {
      auto [records, err] = entry->List(kTimestamp + seconds(3), kTimestamp + seconds(4));
      REQUIRE(err == Error{.code = 404, .message = "No records for time interval"});
      REQUIRE(records.empty());
    }

    SECTION("if there is overlap it is ok") {
      auto records = entry->List(kTimestamp + seconds(1), kTimestamp + seconds(4)).result;
      REQUIRE(records.size() == 2);
      REQUIRE(records[0].time == kTimestamp + seconds(1));
      REQUIRE(records[1].time == kTimestamp + seconds(2));

      records = entry->List(kTimestamp - seconds(1), kTimestamp + seconds(2)).result;
      REQUIRE(records.size() == 2);
      REQUIRE(records[0].time == kTimestamp);
      REQUIRE(records[1].time == kTimestamp + seconds(1));
    }

    SECTION("if timestamp between two blocks") {
      const std::string blob(entry->GetOptions().max_block_size / 2 + 10, 'x');

      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(3)) == Error::kOk);
      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(4)) == Error::kOk);

      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(10)) == Error::kOk);
      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(11)) == Error::kOk);

      REQUIRE(entry->List(kTimestamp + seconds(5), kTimestamp + seconds(12)).error == Error::kOk);
    }
  }
}

TEST_CASE("storage::Entry should not list records which is not finished", "[entry]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  auto writer = entry->BeginWrite(kTimestamp, 1).result;
  REQUIRE(entry->List(kTimestamp - seconds(1), kTimestamp + seconds(1)).result.empty());

  REQUIRE(writer->Write("x") == Error::kOk);
  REQUIRE(entry->List(kTimestamp - seconds(1), kTimestamp + seconds(1)).result.size() == 1);
}

TEST_CASE("storage::Entry should wait when read operations finish before removing block", "[entry][block]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);

  auto [reader, err] = entry->BeginRead(kTimestamp);
  REQUIRE(err == Error::kOk);

  REQUIRE(entry->RemoveOldestBlock() == Error{.code = 500, .message = "Block has active readers"});
  auto [chunk, read_err] = reader->Read();
  REQUIRE(read_err == Error::kOk);

  REQUIRE(entry->RemoveOldestBlock() == Error::kOk);
}

TEST_CASE("storage::Entry should wait when write operations finish before removing block", "[entry][block]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  auto [writer, err] = entry->BeginWrite(kTimestamp, 5);
  REQUIRE(err == Error::kOk);

  REQUIRE(entry->RemoveOldestBlock() == Error{.code = 500, .message = "Block has active writers"});
  REQUIRE(writer->Write("12345", true) == Error::kOk);

  REQUIRE(entry->RemoveOldestBlock() == Error::kOk);
}
