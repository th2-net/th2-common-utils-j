/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.google.protobuf.util.Timestamps.toString

val EventID.logId: String
    get() = "${bookName}:${scope}:${toString(startTimestamp)}:${id}"

val Event.logId: String
    get() = id.logId + (if (hasParentId()) " -> ${parentId.logId}" else "")

val Event.book: String
    get() = id.bookName

val Event.scope: String
    get() = id.scope