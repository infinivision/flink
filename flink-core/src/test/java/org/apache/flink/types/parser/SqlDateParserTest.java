/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.types.parser;


import org.apache.flink.util.TimeConvertUtils;

import java.sql.Date;

public class SqlDateParserTest extends ParserTestBase<Date> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"1970-01-01", "1990-10-14", "2013-08-12", "2040-05-12", "2040-5-12", "1970-1-1",
		};
	}

	@Override
	public Date[] getValidTestResults() {
		return new Date[] {
			new Date(TimeConvertUtils.dateStringToUnixDate("1970-01-01") * TimeConvertUtils.MILLIS_PER_DAY),
			new Date(TimeConvertUtils.dateStringToUnixDate("1990-10-14") * TimeConvertUtils.MILLIS_PER_DAY),
			new Date(TimeConvertUtils.dateStringToUnixDate("2013-08-12") * TimeConvertUtils.MILLIS_PER_DAY),
			new Date(TimeConvertUtils.dateStringToUnixDate("2040-05-12") * TimeConvertUtils.MILLIS_PER_DAY),
			new Date(TimeConvertUtils.dateStringToUnixDate("2040-05-12") * TimeConvertUtils.MILLIS_PER_DAY),
			new Date(TimeConvertUtils.dateStringToUnixDate("1970-01-01") * TimeConvertUtils.MILLIS_PER_DAY)
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			" 2013-08-12", "2013-08-12 ", "2013/08/12", " ", "\t",
			"2013-XX-XX"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<Date> getParser() {
		return new SqlDateParser();
	}

	@Override
	public Class<Date> getTypeClass() {
		return Date.class;
	}
}
