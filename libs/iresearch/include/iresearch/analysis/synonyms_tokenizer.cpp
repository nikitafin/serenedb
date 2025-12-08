#include "synonyms_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iterator>
#include <string_view>

#include "basics/exceptions.h"
#include "basics/result.h"

namespace irs::analysis {

namespace {
SynonymsTokenizer::synonyms_line SplitLine(const std::string_view line) {
  std::set<std::string_view> outputs;
  for (std::string_view s : absl::StrSplit(line, ',')) {
    auto candidate = absl::StripAsciiWhitespace(s);
    if (candidate.empty()) {
      SDB_THROW(sdb::ERROR_VALIDATION_BAD_PARAMETER);
    }
    // @todo unescape
    outputs.insert(absl::StripAsciiWhitespace(s));
  }
  return {std::make_move_iterator(outputs.begin()),
          std::make_move_iterator(outputs.end())};
}
}  // namespace

sdb::ResultOr<SynonymsTokenizer::synonyms_holder> SynonymsTokenizer::parse(
  std::string_view input) {
  synonyms_holder owner;

  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');
  size_t line_number{};
  for (const auto& line : lines) {
    line_number++;
    if (line.empty() || line[0] == '#')
      continue;
    std::vector<std::string_view> sides = absl::StrSplit(line, "=>");
    if (sides.size() > 1) {
      if (sides.size() != 2) {
        return std::unexpected<sdb::Result>{
          std::in_place, sdb::ERROR_VALIDATION_BAD_PARAMETER,
          "More than one explicit mapping specified on the line ", line_number};
      }

      SynonymsTokenizer::synonyms_line outputs;
      try {
        auto inputs = SplitLine(sides[0]);
        outputs = SplitLine(sides[1]);
      } catch (...) {
        return std::unexpected<sdb::Result>{std::in_place,
                                            sdb::ERROR_VALIDATION_BAD_PARAMETER,
                                            "Failed parse line ", line_number};
      }

      owner.push_back(std::move(outputs));

    } else {
      SynonymsTokenizer::synonyms_line outputs;
      try {
        outputs = SplitLine(sides[0]);
      } catch (...) {
        return std::unexpected<sdb::Result>{std::in_place,
                                            sdb::ERROR_VALIDATION_BAD_PARAMETER,
                                            "Failed parse line ", line_number};
      }

      owner.push_back(std::move(outputs));
    }
  }

  // @todo map input -> output

  return owner;
}

sdb::ResultOr<SynonymsTokenizer::synonyms_map> SynonymsTokenizer::parse(
  const synonyms_holder& holder) {
  synonyms_map result;
  for (const auto& line : holder) {
    for (std::string_view synonym : line) {
      result[synonym] = &line;
    }
  }
  return result;
}

SynonymsTokenizer::SynonymsTokenizer(SynonymsTokenizer::synonyms_map&& synonyms)
  : _synonyms(std::move(synonyms)) {}

bool SynonymsTokenizer::next() {
  if (_curr == _end) {
    return false;
  }

  auto& inc = std::get<IncAttr>(_attrs);
  inc.value = (_curr == _begin) ? 1 : 0;

  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(*_curr++);
  return true;
}

bool SynonymsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  if (const auto it = _synonyms.find(data); it == _synonyms.end()) {
    _holder = data;
    _begin = _curr = &_holder;
    _end = _curr + 1;
  } else {
    _begin = _curr = it->second->data();
    _end = _curr + it->second->size();
  }

  return true;
}

}  // namespace irs::analysis
