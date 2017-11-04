/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * A response to a failed request.
 */
public class AdminFailureResponse extends AdminResponse {
  
  private Exception cause;

  /**
   * Returns a {@code AdminFailureResponse} that will be returned to the specified recipient.
   * The message will contains a copy of the local manager's system config.
   */
  public static AdminFailureResponse create(InternalDistributedMember recipient, Exception cause) {
    AdminFailureResponse message = new AdminFailureResponse();
    message.setRecipient(recipient);
    message.cause = cause;
    return message;
  }

  public Exception getCause() {
    return this.cause;
  }

  @Override
  public int getDSFID() {
    return ADMIN_FAILURE_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.cause, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cause = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "AdminFailureResponse from " + getRecipient() + " cause=" + this.cause;
  }
}
