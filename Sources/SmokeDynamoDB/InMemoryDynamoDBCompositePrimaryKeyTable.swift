// swiftlint:disable cyclomatic_complexity
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
//  DynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public protocol PolymorphicDatabaseItemConvertable {
    var createDate: Foundation.Date { get }
    var rowStatus: RowStatus { get }

    func convertToPolymorphicItem<AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes>() throws
        -> PolymorphicDatabaseItem<AttributesType, PossibleTypes>
}

extension TypedDatabaseItem: PolymorphicDatabaseItemConvertable {
    public func convertToPolymorphicItem<TargetAttributesType, PossibleTypes>() throws
        -> PolymorphicDatabaseItem<TargetAttributesType, PossibleTypes> {
        guard let convertedCompositePrimaryKey = compositePrimaryKey as? CompositePrimaryKey<TargetAttributesType> else {
            let description = "Expected to use AttributesType \(TargetAttributesType.self)."
            let context = DecodingError.Context(codingPath: [], debugDescription: description)
            throw DecodingError.typeMismatch(TargetAttributesType.self, context)
        }

        return PolymorphicDatabaseItem<TargetAttributesType, PossibleTypes>(compositePrimaryKey: convertedCompositePrimaryKey,
                                                                            createDate: createDate,
                                                                            rowStatus: rowStatus,
                                                                            rowValue: rowValue)
    }
}

public class InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {

    public var store: [String: [String: PolymorphicDatabaseItemConvertable]] = [:]
    let semaphore = DispatchSemaphore(value: 1)

    public init() {

    }

    public func insertItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        semaphore.wait()
        defer {
            semaphore.signal()
        }
        
        let partition = store[item.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition

            // if the row already exists
            if partition[item.compositePrimaryKey.sortKey] != nil {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: item.compositePrimaryKey.partitionKey,
                                                              sortKey: item.compositePrimaryKey.sortKey,
                                                              message: "Row already exists.")
            }

            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }

        store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }

    public func insertItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                try insertItemSync(item)

                completion(nil)
            } catch {
                completion(error)
            }
    }

    public func clobberItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        semaphore.wait()
        defer {
            semaphore.signal()
        }
        
        let partition = store[item.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition

            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }

        store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }

    public func clobberItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                           completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                try clobberItemSync(item)

                completion(nil)
            } catch {
                completion(error)
            }
    }

    public func updateItemSync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                         existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        semaphore.wait()
        defer {
            semaphore.signal()
        }
        
        let partition = store[newItem.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition

            // if the row already exists
            if let actuallyExistingItem = partition[newItem.compositePrimaryKey.sortKey] {
                if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                    existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                    throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                  sortKey: newItem.compositePrimaryKey.sortKey,
                                                                  message: "Trying to overwrite incorrect version.")
                }
            } else {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                              sortKey: newItem.compositePrimaryKey.sortKey,
                                                              message: "Existing item does not exist.")
            }

            updatedPartition[newItem.compositePrimaryKey.sortKey] = newItem
        } else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                          sortKey: newItem.compositePrimaryKey.sortKey,
                                                          message: "Existing item does not exist.")
        }

        store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
    }

    public func updateItemAsync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                try updateItemSync(newItem: newItem, existingItem: existingItem)

                completion(nil)
            } catch {
                completion(error)
            }
    }

    public func getItemSync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
        -> TypedDatabaseItem<AttributesType, ItemType>? {
            semaphore.wait()
            defer {
                semaphore.signal()
            }
        
            if let partition = store[key.partitionKey] {

                guard let value = partition[key.sortKey] else {
                    return nil
                }

                guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                    let foundType = type(of: value)
                    let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)
                    throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                }

                return item
            }

            return nil
    }

    public func getItemAsync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                       completion: @escaping (SmokeDynamoDBErrorResult<TypedDatabaseItem<AttributesType, ItemType>?>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                let item: TypedDatabaseItem<AttributesType, ItemType>? = try getItemSync(forKey: key)

                completion(.success(item))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }

    public func deleteItemSync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws {
        semaphore.wait()
        defer {
            semaphore.signal()
        }
        
        store[key.partitionKey]?[key.sortKey] = nil
    }

    public func deleteItemAsync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            do {
                try deleteItemSync(forKey: key)

                completion(nil)
            } catch {
                completion(error)
            }
    }
    
    public func deleteItemSync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        semaphore.wait()
        defer {
            semaphore.signal()
        }
        
        let partition = store[existingItem.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition

            // if the row already exists
            if let actuallyExistingItem = partition[existingItem.compositePrimaryKey.sortKey] {
                if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                    throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                  sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                  message: "Trying to delete incorrect version.")
                }
            } else {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                              sortKey: existingItem.compositePrimaryKey.sortKey,
                                                              message: "Existing item does not exist.")
            }

            updatedPartition[existingItem.compositePrimaryKey.sortKey] = nil
        } else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                          sortKey: existingItem.compositePrimaryKey.sortKey,
                                                          message: "Existing item does not exist.")
        }

        store[existingItem.compositePrimaryKey.partitionKey] = updatedPartition
    }
    
    public func deleteItemAsync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ()) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        do {
            try deleteItemSync(existingItem: existingItem)

            completion(nil)
        } catch {
            completion(error)
        }
    }

    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] {
        semaphore.wait()
        defer {
            semaphore.signal()
        }
        
        var items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = []

        if let partition = store[partitionKey] {
            let sortedPartition = partition.sorted(by: { (left, right) -> Bool in
                return left.key < right.key
            })
            
            sortKeyIteration: for (sortKey, value) in sortedPartition {

                if let currentSortKeyCondition = sortKeyCondition {
                    switch currentSortKeyCondition {
                    case .equals(let value):
                        if !(value == sortKey) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThan(let value):
                        if !(sortKey < value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThanOrEqual(let value):
                        if !(sortKey <= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThan(let value):
                        if !(sortKey > value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThanOrEqual(let value):
                        if !(sortKey >= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .between(let value1, let value2):
                        if !(sortKey > value1 && sortKey < value2) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .beginsWith(let value):
                        if !(sortKey.hasPrefix(value)) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    }
                }

                items.append(try value.convertToPolymorphicItem())
            }
        }

        return items
    }

    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (SmokeDynamoDBErrorResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            do {
                let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] =
                    try querySync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition)

                completion(.success(items))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
    
    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int?,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            return try querySync(forPartitionKey: partitionKey,
                                 sortKeyCondition: sortKeyCondition,
                                 limit: limit,
                                 scanIndexForward: true,
                                 exclusiveStartKey: exclusiveStartKey)
    }

    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?,
                                                         limit: Int?,
                                                         scanIndexForward: Bool,
                                                         exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            // get all the results
            let rawItems: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = try querySync(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition)
            
            let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>]
            if !scanIndexForward {
                items = rawItems.reversed()
            } else {
                items = rawItems
            }

            let startIndex: Int
            // if there is an exclusiveStartKey
            if let exclusiveStartKey = exclusiveStartKey {
                guard let storedStartIndex = Int(exclusiveStartKey) else {
                    fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
                }

                startIndex = storedStartIndex
            } else {
                startIndex = 0
            }

            let endIndex: Int
            let lastEvaluatedKey: String?
            if let limit = limit, startIndex + limit < items.count {
                endIndex = startIndex + limit
                lastEvaluatedKey = String(endIndex)
            } else {
                endIndex = items.count
                lastEvaluatedKey = nil
            }

            return (Array(items[startIndex..<endIndex]), lastEvaluatedKey)
    }

    private func getItemAsPolymorphicDatabaseItemConvertable<ConvertableType>(value: Any) throws
        -> ConvertableType where ConvertableType: PolymorphicDatabaseItemConvertable {
        guard let polymorphicDatabaseItemConvertable = value as? ConvertableType else {
            let description = "Expected to decode \(ConvertableType.self). Instead found \(value.self)."
            let context = DecodingError.Context(codingPath: [], debugDescription: description)
            throw DecodingError.typeMismatch(ConvertableType.self, context)
        }

        return polymorphicDatabaseItemConvertable
    }

    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            do {
                let result: ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?) =
                    try querySync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition,
                                  limit: limit,
                                  scanIndexForward: true,
                                  exclusiveStartKey: exclusiveStartKey)

                completion(.success(result))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
    
    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            do {
                let result: ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?) =
                    try querySync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition,
                                  limit: limit,
                                  scanIndexForward: scanIndexForward,
                                  exclusiveStartKey: exclusiveStartKey)

                completion(.success(result))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
}
