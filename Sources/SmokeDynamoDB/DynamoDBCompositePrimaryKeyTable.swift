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

/**
 Enumeration of the errors that can be thrown by a DynamoDBTable.
 */
public enum SmokeDynamoDBError: Error {
    case databaseError(reason: String)
    case unexpectedError(cause: Swift.Error)
    case dynamoDBError(cause: DynamoDBError)
    case unexpectedResponse(reason: String)
    case conditionalCheckFailed(partitionKey: String, sortKey: String, message: String?)
    case typeMismatch(expected: String, provided: String)
    case unexpectedType(provided: String)
    case concurrencyError(partitionKey: String, sortKey: String, message: String?)
    case unableToUpdateError(reason: String)
    case unrecognizedError(String, String?)
}

public typealias SmokeDynamoDBErrorResult<SuccessPayload> = Result<SuccessPayload, SmokeDynamoDBError>

public extension Swift.Error {
    func asUnrecognizedSmokeDynamoDBError() -> SmokeDynamoDBError {
        let errorType = String(describing: type(of: self))
        let errorDescription = String(describing: self)
        return .unrecognizedError(errorType, errorDescription)
    }
}

public extension DynamoDBError {
    func asSmokeDynamoDBError() -> SmokeDynamoDBError {
        return .dynamoDBError(cause: self)
    }
}

/**
 Enumeration of the types of conditions that can be specified for an attribute.
 */
public enum AttributeCondition {
    case equals(String)
    case lessThan(String)
    case lessThanOrEqual(String)
    case greaterThan(String)
    case greaterThanOrEqual(String)
    case between(String, String)
    case beginsWith(String)
}

public protocol DynamoDBCompositePrimaryKeyTable {

    /**
     * Insert item is a non-destructive API. If an item already exists with the specified key this
     * API should fail.
     */
    func insertItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws

    func insertItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                   completion: @escaping (Error?) -> ()) throws

    /**
     * Clobber item is destructive API. Regardless of what is present in the database the provided
     * item will be inserted.
     */
    func clobberItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws

    func clobberItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                    completion: @escaping (Error?) -> ()) throws

    /**
     * Update item requires having gotten an item from the database previously and will not update
     * if the item at the specified key is not the existing item provided.
     */
    func updateItemSync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                  existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws

    func updateItemAsync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                   existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                   completion: @escaping (Error?) -> ()) throws

    /**
     * Retrieves an item from the database table. Returns nil if the item doesn't exist.
     */
    func getItemSync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> TypedDatabaseItem<AttributesType, ItemType>?

    func getItemAsync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                completion: @escaping (SmokeDynamoDBErrorResult<TypedDatabaseItem<AttributesType, ItemType>?>)  -> ()) throws

    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. 
     */
    func deleteItemSync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws

    func deleteItemAsync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                         completion: @escaping (Error?) -> ()) throws
    
    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. This operation will not modify the table
     * if the item at the specified key is not the existing item provided.
     */
    func deleteItemSync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws

    func deleteItemAsync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                         completion: @escaping (Error?) -> ()) throws

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<AttributesType, PossibleTypes>]

    func queryAsync<AttributesType, PossibleTypes>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        completion: @escaping (SmokeDynamoDBErrorResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ()) throws

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int?,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)

    func queryAsync<AttributesType, PossibleTypes>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?,
        completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ()) throws
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int?,
                                                  scanIndexForward: Bool,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)

    func queryAsync<AttributesType, PossibleTypes>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?,
        completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ()) throws
    
    // MARK: Monomorphic queries
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                        sortKeyCondition: AttributeCondition?) throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]

    func monomorphicQueryAsync<AttributesType, ItemType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        completion: @escaping (SmokeDynamoDBErrorResult<[TypedDatabaseItem<AttributesType, ItemType>]>) -> ()) throws
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                        sortKeyCondition: AttributeCondition?,
                                                        limit: Int?,
                                                        scanIndexForward: Bool,
                                                        exclusiveStartKey: String?) throws
        -> ([TypedDatabaseItem<AttributesType, ItemType>], String?)

    func monomorphicQueryAsync<AttributesType, ItemType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?,
        completion: @escaping (SmokeDynamoDBErrorResult<([TypedDatabaseItem<AttributesType, ItemType>], String?)>) -> ()) throws
}
