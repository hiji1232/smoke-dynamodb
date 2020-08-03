// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// A copy of the License is located at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//  DynamoDBCompositePrimaryKeysProjection+asyncExtensions.swift
//  SmokeDynamoDB
//

import NIO

public extension DynamoDBCompositePrimaryKeysProjection {
    
    func queryAsync<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            on eventLoop: EventLoop) -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>
            where AttributesType: PrimaryKeyAttributes {
        let (promise, completion) = EventLoopPromise<[CompositePrimaryKey<AttributesType>]>
                .forResultCompletionHandler(on: eventLoop)
            
        do {
            try queryAsync(
                forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }
    
    func queryAsync<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([CompositePrimaryKey<AttributesType>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            try queryAsync(forPartitionKey: partitionKey,
                           sortKeyCondition: sortKeyCondition,
                           limit: limit,
                           scanIndexForward: true,
                           exclusiveStartKey: exclusiveStartKey,
                           completion: completion)
    }
    
    func queryAsync<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            on eventLoop: EventLoop) -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        let (promise, completion) = EventLoopPromise<([CompositePrimaryKey<AttributesType>], String?)>
                .forResultCompletionHandler(on: eventLoop)
            
        do {
            try queryAsync(
                forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, limit: limit,
                scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }
}
