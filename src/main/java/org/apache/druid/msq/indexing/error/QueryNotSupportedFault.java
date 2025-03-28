/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;

@JsonTypeName(QueryNotSupportedFault.CODE)
public class QueryNotSupportedFault extends BaseMSQFault
{
  public static final QueryNotSupportedFault INSTANCE = new QueryNotSupportedFault();
  static final String CODE = "QueryNotSupported";

  QueryNotSupportedFault()
  {
    super(CODE);
  }

  private QueryNotSupportedFault(@Nullable String errorMessage)
  {
    super(CODE, errorMessage);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @Override
  public DruidException toDruidException()
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.UNSUPPORTED)
                         .withErrorCode(getErrorCode())
                         .build(MSQFaultUtils.generateMessageWithErrorCode(this));
  }

  @JsonCreator
  public static QueryNotSupportedFault instance()
  {
    return INSTANCE;
  }

  public static class Builder
  {
    @Nullable
    private String errorMessage;

    public Builder withErrorMessage(@Nullable String errorMessage)
    {
      this.errorMessage = errorMessage;
      return this;
    }

    public QueryNotSupportedFault build()
    {
      return new QueryNotSupportedFault(errorMessage);
    }
  }
}
