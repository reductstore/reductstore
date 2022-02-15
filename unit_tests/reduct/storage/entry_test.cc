// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::core::Error;
using reduct::storage::IEntry;
using std::chrono::seconds;

static auto MakeDefaultOptions() {
  return IEntry::Options{
      .name = "entry_1",
      .path = BuildTmpDirectory(),
      .max_block_size = 100,
  };
}

static const auto kTimestamp = IEntry::Time();

TEST_CASE("storage::Entry should record file to a block", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(entry->Write("some_data", kTimestamp) == Error::kOk);

  SECTION("one record") {
    auto ret = entry->Read(kTimestamp);
    REQUIRE(ret == IEntry::ReadResult{"some_data", Error{}, kTimestamp});
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 1, .bytes = 11});
  }

  SECTION("few records") {
    REQUIRE(entry->Write("other_data1", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->Write("other_data2", kTimestamp + seconds(10)) == Error::kOk);
    REQUIRE(entry->Write("other_data3", kTimestamp + seconds(15)) == Error::kOk);

    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 4, .bytes = 50});

    auto ret = entry->Read(kTimestamp);
    REQUIRE(ret.blob == "some_data");

    ret = entry->Read(kTimestamp + seconds(15));
    REQUIRE(ret.blob == "other_data3");
  }

  SECTION("error 404 if request out of  interval") {
    auto bad_result = entry->Read(kTimestamp - seconds(10));
    REQUIRE(bad_result.error == Error{.code = 404, .message = "No records for this timestamp"});
  }
}

TEST_CASE("storage::Entry should create a new block if the current > max_block_size", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(entry->Write(std::string(100, 'c'), kTimestamp) == Error::kOk);

  SECTION("one record") {
    auto ret = entry->Read(kTimestamp);
    REQUIRE(ret == IEntry::ReadResult{std::string(100, 'c'), Error{}, kTimestamp});
    REQUIRE(entry->GetInfo() == IEntry::Info{
                                    .block_count = 1,
                                    .record_count = 1,
                                    .bytes = 102,
                                    .oldest_record_time = kTimestamp,
                                    .latest_record_time = kTimestamp,
                                });
  }

  SECTION("two records in different blocks") {
    REQUIRE(entry->Write("other_data1", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 2, .record_count = 2, .bytes = 115});

    auto ret = entry->Read(kTimestamp + seconds(5));
    REQUIRE(ret.blob == "other_data1");
  }
}

TEST_CASE("storage::Entry should write data for random kTimestamp", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(entry->Write(std::string(100, 'c'), kTimestamp) == Error::kOk);

  SECTION("a record older than first in entry") {
    REQUIRE(entry->Write("belated_data", kTimestamp - seconds(5)) == Error::kOk);
    REQUIRE(entry->Read(kTimestamp - seconds(5)) ==
            IEntry::ReadResult{"belated_data", Error::kOk, kTimestamp - seconds(5)});
  }

  SECTION("a belated record") {
    REQUIRE(entry->GetInfo().block_count == 1);
    REQUIRE(entry->Write("latest_data", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->Write("latest_data", kTimestamp + seconds(15)) == Error::kOk);
    REQUIRE(entry->Write("belated_data", kTimestamp + seconds(10)) == Error::kOk);

    REQUIRE(entry->Read(kTimestamp + seconds(10)) ==
            IEntry::ReadResult{"belated_data", Error::kOk, kTimestamp + seconds(10)});
  }
}

TEST_CASE("storage::Entry should restore itself from descriptors", "[entry]") {
  const auto options = MakeDefaultOptions();
  auto entry = IEntry::Build(options);
  REQUIRE(entry);

  REQUIRE(entry->Write("some_data", kTimestamp) == Error::kOk);

  entry = IEntry::Restore(options.path / options.name);
  REQUIRE(entry->GetOptions() == options);

  REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 1, .bytes = 11});

  SECTION("should work ok after restoring") {
    REQUIRE(entry->Write("next_data", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->Read(kTimestamp + seconds(5)) ==
            IEntry::ReadResult{.blob = "next_data", .error = Error::kOk, .time = kTimestamp + seconds(5)});
  }
}

TEST_CASE("storage::Entry should read from empty entry with 404", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());

  REQUIRE(entry);
  REQUIRE(entry->Read(IEntry::Time()).error.code == 404);
}

TEST_CASE("storage::Entry should remove last block", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  const std::string blob(entry->GetOptions().max_block_size, 'x');
  REQUIRE(entry->Write(blob, kTimestamp) == Error::kOk);
  REQUIRE(entry->Write(blob, kTimestamp + seconds(1)) == Error::kOk);
  REQUIRE(entry->Write(blob, kTimestamp + seconds(2)) == Error::kOk);
  REQUIRE(entry->GetInfo().block_count == 3);

  SECTION("remove one block") {
    REQUIRE(entry->RemoveOldestBlock() == Error::kOk);
    REQUIRE(entry->GetInfo().block_count == 2);
    REQUIRE(entry->Read(kTimestamp).error.code == 404);

    SECTION("write should be ok") {
      REQUIRE(entry->Write("some_data", kTimestamp) == Error::kOk);
      REQUIRE(entry->Read(kTimestamp).error == Error::kOk);
    }

    SECTION("remove two blocks") {
      REQUIRE(entry->RemoveOldestBlock() == Error::kOk);
      REQUIRE(entry->GetInfo().block_count == 1);
      REQUIRE(entry->Read(kTimestamp + seconds(1)).error.code == 404);
    }

    SECTION("recovery") {
      auto info = entry->GetInfo();
      entry = IEntry::Restore(entry->GetOptions().path / "entry_1");

      REQUIRE(entry);
      REQUIRE(entry->GetInfo() == info);
    }
  }
}

TEST_CASE("storage::Entry should list records for time interval", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  SECTION("empty record") {
    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(1));
    REQUIRE(err == Error{.code = 404, .message = "No records in the entry"});
    REQUIRE(records.empty());
  }

  SECTION("some records in few blocks") {
    const std::string blob(entry->GetOptions().max_block_size, 'x');
    REQUIRE(entry->Write(blob, kTimestamp) == Error::kOk);
    REQUIRE(entry->Write(blob, kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(entry->Write(blob, kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 102});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 102});
  }

  SECTION("some records in one block") {
    REQUIRE(entry->Write("blob", kTimestamp) == Error::kOk);
    REQUIRE(entry->Write("blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(entry->Write("blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 6});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 6});
  }

  SECTION("list should be sorted") {
    REQUIRE(entry->Write("blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(entry->Write("blob", kTimestamp) == Error::kOk);
    REQUIRE(entry->Write("blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 6});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 6});
  }

  SECTION("extreme cases") {
    REQUIRE(entry->Write("blob", kTimestamp) == Error::kOk);
    REQUIRE(entry->Write("blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(entry->Write("blob", kTimestamp + seconds(2)) == Error::kOk);

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
      auto records = entry->List(kTimestamp + seconds(1), kTimestamp + seconds(4)).records;
      REQUIRE(records.size() == 2);
      REQUIRE(records[0].time == kTimestamp + seconds(1));
      REQUIRE(records[1].time == kTimestamp + seconds(2));

      records = entry->List(kTimestamp - seconds(1), kTimestamp + seconds(2)).records;
      REQUIRE(records.size() == 2);
      REQUIRE(records[0].time == kTimestamp);
      REQUIRE(records[1].time == kTimestamp + seconds(1));
    }

    SECTION("if timestamp between two blocks") {
      const std::string blob(entry->GetOptions().max_block_size / 2 + 10, 'x');

      REQUIRE(entry->Write(blob, kTimestamp + seconds(3)) == Error::kOk);
      REQUIRE(entry->Write(blob, kTimestamp + seconds(4)) == Error::kOk);

      REQUIRE(entry->Write(blob, kTimestamp + seconds(10)) == Error::kOk);
      REQUIRE(entry->Write(blob, kTimestamp + seconds(11)) == Error::kOk);

      REQUIRE(entry->List(kTimestamp + seconds(5), kTimestamp + seconds(12)).error == Error::kOk);
    }
  }
}
