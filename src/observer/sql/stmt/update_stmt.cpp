/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/select_stmt.h"

UpdateStmt::UpdateStmt(Table *table, Value *values, FieldMeta fields, FilterStmt *filter_stmt)
    : table_(table), values_(values), fields_(fields), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (filter_stmt_ != nullptr) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  const char *table_name = update.relation_name.c_str();
  if (db == nullptr || table_name == nullptr) {
    LOG_WARN("invalid argument. db=%p, table_name=%p",db, table_name);
    return RC::INVALID_ARGUMENT;
  }
  Table *table = db->find_table(table_name);
  if (table == nullptr) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  const TableMeta &table_meta   = table->table_meta();
  const FieldMeta *update_field = table_meta.field(update.attribute_name.c_str());
  if (update_field == nullptr)
    return RC::INVALID_ARGUMENT;
  if (update_field->type() != update.value.attr_type()) {
    LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
            table->name(), update_field->name(), update_field->type(), update.value.attr_type());
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }
  stmt = new UpdateStmt(table, const_cast<Value *>(&update.value), *update_field, filter_stmt);
  return RC::SUCCESS;
}
