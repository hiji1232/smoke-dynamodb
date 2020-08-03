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
//  EventLoopPromise+forCompletionHandler.swift
//  SmokeDynamoDB
//

import NIO

extension EventLoopPromise {
    
    static func forNoResultCompletionHandler(on eventLoop: EventLoop)
            -> (EventLoopPromise<Void>, (Error?) -> ()) {
        let promise = eventLoop.makePromise(of: Void.self)
        
        func completion(error: Error?) {
            if let error = error {
                promise.fail(error)
            } else {
                promise.succeed(())
            }
        }
        
        return (promise, completion)
    }
    
    static func forResultCompletionHandler(on eventLoop: EventLoop)
            -> (EventLoopPromise<Value>, (SmokeDynamoDBErrorResult<Value>) -> ()) {
        let promise = eventLoop.makePromise(of: Value.self)
        
        func completion(result: SmokeDynamoDBErrorResult<Value>) {
            switch result {
            case .failure(let error):
                promise.fail(error)
            case .success(let value):
                promise.succeed(value)
            }
        }
        
        return (promise, completion)
    }
}
