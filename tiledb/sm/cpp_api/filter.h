/**
 * @file   filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements the C++ API for the TileDB Filter object.
 */

#ifndef TILEDB_CPP_API_FILTER_H
#define TILEDB_CPP_API_FILTER_H

#include "tiledb.h"

#include <iostream>
#include <string>

namespace tiledb {

/**
 * Represents a filter. A filter is used to transform attribute data e.g.
 * with compression, delta encoding, etc.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
 * f.set_option(TILEDB_COMPRESSION_LEVEL, 5);
 * @endcode
 */
class Filter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a Filter of the given type.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * @endcode
   *
   * @param ctx TileDB context
   * @param filter_type Enumerated type of filter
   */
  Filter(const Context& ctx, tiledb_filter_type_t filter_type)
      : ctx_(ctx) {
    tiledb_filter_t* filter;
    ctx.handle_error(tiledb_filter_alloc(ctx, filter_type, &filter));
    filter_ = std::shared_ptr<tiledb_filter_t>(filter, deleter_);
  }

  /**
   * Creates a Filter with the input C object.
   *
   * @param ctx TileDB context
   * @param filter C API filter object
   */
  Filter(const Context& ctx, tiledb_filter_t* filter)
      : ctx_(ctx) {
    filter_ = std::shared_ptr<tiledb_filter_t>(filter, deleter_);
  }

  Filter() = default;
  Filter(const Filter&) = default;
  Filter(Filter&& o) = default;
  Filter& operator=(const Filter&) = default;
  Filter& operator=(Filter&& o) = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_filter_t*() const {
    return filter_.get();
  }

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_filter_t> ptr() const {
    return filter_;
  }

  /**
   * Sets an option on the filter. Options are filter dependent; this function
   * throws an error if the given option is not valid for the given filter.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * f.set_option(TILEDB_COMPRESSION_LEVEL, 5);
   * @endcode
   *
   * @param option Enumerated option to set.
   * @param value Value of option to set.
   * @return Reference to this Filter
   *
   * @throws TileDBError if the option cannot be set on the filter.
   */
  Filter& set_option(tiledb_filter_option_t option, const void* value) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_filter_set_option(ctx, filter_.get(), option, value));
    return *this;
  }

  /**
   * Gets an option value from the filter.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * int32_t level;
   * f.get_option(TILEDB_COMPRESSION_LEVEL, &level);
   * // level == -1 (the default compression level)
   * @endcode
   *
   * @param option Enumerated option to get.
   * @param value Buffer that option value will be written to.
   *
   * @note The buffer pointed to by `value` must be large enough to hold the
   *    option value.
   *
   * @throws TileDBError if the option cannot be retrieved from the filter.
   */
  void get_option(tiledb_filter_option_t option, void* value) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_filter_get_option(ctx, filter_.get(), option, value));
  }

  /** Gets the filter type of this filter. */
  tiledb_filter_type_t filter_type() const {
    auto& ctx = ctx_.get();
    tiledb_filter_type_t type;
    ctx.handle_error(tiledb_filter_get_type(ctx, filter_.get(), &type));
    return type;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Returns the input type in string format. */
  static std::string to_str(tiledb_filter_type_t type) {
    switch (type) {
      case TILEDB_FILTER_NONE:
        return "NOOP";
      case TILEDB_FILTER_GZIP:
        return "GZIP";
      case TILEDB_FILTER_ZSTD:
        return "ZSTD";
      case TILEDB_FILTER_LZ4:
        return "LZ4";
      case TILEDB_FILTER_RLE:
        return "RLE";
      case TILEDB_FILTER_BZIP2:
        return "BZIP2";
      case TILEDB_FILTER_DOUBLE_DELTA:
        return "DOUBLE_DELTA";
      case TILEDB_FILTER_BIT_WIDTH_REDUCTION:
        return "BIT_WIDTH_REDUCTION";
      case TILEDB_FILTER_BITSHUFFLE:
        return "BITSHUFFLE";
      case TILEDB_FILTER_BYTESHUFFLE:
        return "BYTESHUFFLE";
      case TILEDB_FILTER_POSITIVE_DELTA:
        return "POSITIVE_DELTA";
    }
    return "";
  }

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB filter object. */
  std::shared_ptr<tiledb_filter_t> filter_;
};

/** Gets a string representation of a filter for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Filter& f) {
  os << "Filter<" << Filter::to_str(f.filter_type()) << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_FILTER_H
