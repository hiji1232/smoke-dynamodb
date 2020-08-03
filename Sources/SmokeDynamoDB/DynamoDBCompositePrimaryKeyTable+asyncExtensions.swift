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
//  DynamoDBCompositePrimaryKeyTable+AsyncExtensions.swift
//  SmokeDynamoDB
//

import NIO
import DynamoDBModel

public extension DynamoDBCompositePrimaryKeyTable {
    
    func insertItemAsync<AttributesType, ItemType>(
            _ item: TypedDatabaseItem<AttributesType, ItemType>,
            on eventLoop: EventLoop) -> EventLoopFuture<Void>
        where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
            
            do {
                try insertItemAsync(item, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    func clobberItemAsync<AttributesType, ItemType>(
            _ item: TypedDatabaseItem<AttributesType, ItemType>,
            on eventLoop: EventLoop) -> EventLoopFuture<Void>
        where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
            
            do {
                try clobberItemAsync(item, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    func updateItemAsync<AttributesType, ItemType>(
            newItem: TypedDatabaseItem<AttributesType, ItemType>,
            existingItem: TypedDatabaseItem<AttributesType, ItemType>,
            on eventLoop: EventLoop) -> EventLoopFuture<Void>
        where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
            
            do {
                try updateItemAsync(newItem: newItem, existingItem: existingItem, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    func getItemAsync<AttributesType, ItemType>(
            forKey key: CompositePrimaryKey<AttributesType>,
            on eventLoop: EventLoop) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?>
        where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let (promise, completion) = EventLoopPromise<TypedDatabaseItem<AttributesType, ItemType>?>
                .forResultCompletionHandler(on: eventLoop)
            
            do {
                try getItemAsync(forKey: key, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    func deleteItemAsync<AttributesType>(
            forKey key: CompositePrimaryKey<AttributesType>,
            on eventLoop: EventLoop) -> EventLoopFuture<Void>
        where AttributesType: PrimaryKeyAttributes {
            let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
            
            do {
                try deleteItemAsync(forKey: key, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            on eventLoop: EventLoop) -> EventLoopFuture<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let (promise, completion) = EventLoopPromise<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>
                .forResultCompletionHandler(on: eventLoop)
            
            do {
                try queryAsync(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    
    
    func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?,
            on eventLoop: EventLoop) -> EventLoopFuture<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            queryAsync(forPartitionKey: partitionKey,
                       sortKeyCondition: sortKeyCondition,
                       limit: limit,
                       scanIndexForward: true,
                       exclusiveStartKey: exclusiveStartKey,
                       on: eventLoop)
    }
    
    func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, scanIndexForward: Bool, exclusiveStartKey: String?,
            on eventLoop: EventLoop) -> EventLoopFuture<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let (promise, completion) = EventLoopPromise<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>
                .forResultCompletionHandler(on: eventLoop)
            
            do {
                try queryAsync(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, limit: limit,
                               scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey, completion: completion)
            } catch {
                promise.fail(error)
            }
            
            return promise.futureResult
    }
    
    func clobberVersionedItemWithHistoricalRowAsync<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(
            forPrimaryKey partitionKey: String,
            andHistoricalKey historicalKey: String,
            item: ItemType,
            primaryKeyType: AttributesType.Type,
            generateSortKey: @escaping (Int) -> String,
            on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
        
        do {
            try clobberVersionedItemWithHistoricalRowAsync(
                forPrimaryKey: partitionKey, andHistoricalKey: historicalKey,
                item: item, primaryKeyType: primaryKeyType,
                generateSortKey: generateSortKey, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }
       
    func conditionallyUpdateItemAsync<AttributesType, ItemType: Codable>(
            forKey key: CompositePrimaryKey<AttributesType>,
            withRetries retries: Int = 10,
            updatedPayloadProvider: @escaping (ItemType) throws -> ItemType,
            on eventLoop: EventLoop) -> EventLoopFuture<Void> {
       let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
       
       do {
           try conditionallyUpdateItemAsync(
               forKey: key, withRetries: retries,
               updatedPayloadProvider: updatedPayloadProvider, completion: completion)
       } catch {
           promise.fail(error)
       }
       
       return promise.futureResult
   }

    func insertItemWithHistoricalRowAsync<AttributesType, ItemType>(
            primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
            historicalItem: TypedDatabaseItem<AttributesType, ItemType>,
            on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
        
        do {
            try insertItemWithHistoricalRowAsync(
                primaryItem: primaryItem, historicalItem: historicalItem, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }

    func updateItemWithHistoricalRowAsync<AttributesType, ItemType>(
            primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
            existingItem: TypedDatabaseItem<AttributesType, ItemType>,
            historicalItem: TypedDatabaseItem<AttributesType, ItemType>,
            on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
        
        do {
            try updateItemWithHistoricalRowAsync(
                primaryItem: primaryItem, existingItem: existingItem,
                historicalItem: historicalItem, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }

    func clobberItemWithHistoricalRowAsync<AttributesType, ItemType>(
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>?) -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10,
            on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let (promise, completion) = EventLoopPromise<Void>.forNoResultCompletionHandler(on: eventLoop)
        
        do {
            try clobberItemWithHistoricalRowAsync(
                primaryItemProvider: primaryItemProvider, historicalItemProvider: historicalItemProvider,
                withRetries: retries, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }

    func conditionallyUpdateItemWithHistoricalRowAsync<AttributesType, ItemType>(
            forPrimaryKey compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10,
            on eventLoop: EventLoop) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
        let (promise, completion) = EventLoopPromise<TypedDatabaseItem<AttributesType, ItemType>>
            .forResultCompletionHandler(on: eventLoop)
        
        do {
            try conditionallyUpdateItemWithHistoricalRowAsync(
                forPrimaryKey: compositePrimaryKey, primaryItemProvider: primaryItemProvider,
                historicalItemProvider: historicalItemProvider, withRetries: retries, completion: completion)
        } catch {
            promise.fail(error)
        }
        
        return promise.futureResult
    }
}
