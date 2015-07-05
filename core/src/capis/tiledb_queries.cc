/**
 * @file   tiledb_queries.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the C APIs of the TileDB queries.
 */

#include "array_schema.h"
#include "data_generator.h"
#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include "tiledb_error.h"
#include "tiledb_queries.h"
#include <assert.h>

typedef struct TileDB_CTX{
  Loader* loader_;
  QueryProcessor* query_processor_;
  StorageManager* storage_manager_;
} TileDB_CTX;

int tiledb_clear_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {
  return tiledb_ctx->storage_manager_->clear_array(array_name);
}

int tiledb_define_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array_schema_str) {
  // Creare array schema from the input string 
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->deserialize(array_schema_str)) {
    std::cerr << ERROR_MSG_HEADER << " Failed to parse array schema.\n";
    return TILEDB_EPARRSCHEMA;
  }

  // Define the array
  if(tiledb_ctx->storage_manager_->define_array(array_schema)) { 
    std::cerr << ERROR_MSG_HEADER << " Failed to define array.\n";
// TODO: Print better message
    return TILEDB_EDEFARR;
  }

  // Clean up 
  delete array_schema;

  return 0;
} 

int tiledb_delete_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {
  return tiledb_ctx->storage_manager_->delete_array(array_name);
}

int tiledb_export_csv(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* filename,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    bool reverse) {
  // Compute vectors for dim_names and attribute names 
  std::vector<std::string> dim_names_vec, attribute_names_vec;
  for(int i=0; i<dim_names_num; ++i) 
    dim_names_vec.push_back(dim_names[i]);
  for(int i=0; i<attribute_names_num; ++i) 
    attribute_names_vec.push_back(attribute_names[i]);

  return tiledb_ctx->query_processor_->export_csv(
      array_name, filename, dim_names_vec, attribute_names_vec, reverse);
}

int tiledb_generate_data(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* filename,
    const char* filetype,
    unsigned int seed,
    int64_t cell_num) {
  // Check cell_num
  if(cell_num <= 0) {
    std::cerr << ERROR_MSG_HEADER 
             << " The number of cells must be a positive integer.\n";
    return TILEDB_EIARG;
  }

  // To handle return codes
  int rc;

  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  rc = tiledb_ctx->storage_manager_->get_array_schema(
           array_name, array_schema);
  if(rc) 
    return rc;

  // Generate the file
  DataGenerator data_generator(array_schema);
  if(strcmp(filetype, "csv") == 0) {
    rc = data_generator.generate_csv(seed, filename, cell_num);
  } else if(strcmp(filetype, "bin") == 0) {
    rc = data_generator.generate_bin(seed, filename, cell_num);
  } else {
    std::cerr << ERROR_MSG_HEADER 
             << " Unknown file type '" << filetype << "'.\n";
    return TILEDB_EIARG;
  }

  // Handle errors 
  if(rc) 
    return rc;

  // Clean up
  delete array_schema; 

  return 0;
}

int tiledb_load_bin(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    bool sorted) {
  return tiledb_ctx->loader_->load_bin(array_name, path, sorted);
}

int tiledb_load_csv(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    bool sorted) {
  return tiledb_ctx->loader_->load_csv(array_name, path, sorted);
}
    
int tiledb_show_array_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {
  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  int rc = tiledb_ctx->storage_manager_->get_array_schema(
               array_name, array_schema);
  if(rc) 
    return rc;

  // Print array schema
  array_schema->print();

  // Clean up
  delete array_schema; 

  return 0;
}

int tiledb_subarray(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* result_name,
    const double* range,
    int range_size,
    const char** attribute_names,
    int attribute_names_num) {
  // Compute vectors for range and attribute names 
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i) 
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<double> range_vec;
  for(int i=0; i<range_size; ++i) 
    range_vec.push_back(range[i]);

  return tiledb_ctx->query_processor_->subarray(
      array_name, range_vec, result_name, attribute_names_vec);
}

int tiledb_update_bin(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    bool sorted) {
  return tiledb_ctx->loader_->update_bin(array_name, path, sorted);
}

int tiledb_update_csv(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    bool sorted) {
  return tiledb_ctx->loader_->update_csv(array_name, path, sorted);
}
