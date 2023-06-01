/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import Foundation
import SwiftyJSON

public enum CozoError: Error {
    case system(String)
    case query(JSON)
}

public class RowHeaders {
    public let headers: [String]
    init(headers: [String]) {
        self.headers = headers
    }
    lazy var invertedKeys: [String:Int] = {
        var ret: [String:Int] = [:]
        for i in 0..<self.headers.count {
            ret[self.headers[i]] = i
        }
        return ret
    }()
}

public struct NamedRow {
    public let headers: RowHeaders
    public let fields: [JSON]
    
    public func get(idx: Int) -> JSON {
        return self.fields[idx]
    }
    public func get(key: String) -> JSON? {
        if let idx = self.headers.invertedKeys[key] {
            return self.fields[idx]
        } else {
            return nil
        }
    }
}

extension [NamedRow] {
    public func toString() -> String {
        var ret = "["
        var isFirst = true
        for field in self {
            if !isFirst {
                ret += ", "
            } else {
                isFirst = false
            }
            ret += field.toString()
        }
        ret += "]"
        return ret
    }
}

extension NamedRow {
    public func toString() -> String {
        var ret = "{"
        for i in 0..<self.headers.headers.count {
            if i != 0 {
                ret += ", "
            }
            ret += self.headers.headers[i]
            ret += ": "
            ret += self.fields[i].rawString(.utf8, options: .init(rawValue: 0))!
        }
        ret += "}"
        return ret
    }
}

public class CozoDB {
    public let db: DbInstance
    
    public init() {
        let db = new_cozo_db("mem", "", "")!
        self.db = db
    }
    public init(kind: String, path: String) throws {
        if let db = new_cozo_db(kind, path, "") {
            self.db = db
        } else {
            throw CozoError.system("Cannot create database")
        }
    }
    public func run(_ query: String, params: JSON) throws -> [NamedRow] {
        let payload = params.rawString(.utf8, options: .init(rawValue: 0))!
        return try self.run(query, stringParams: payload)
    }
    public func run(_ query: String) throws -> [NamedRow] {
        return try self.run(query, stringParams: "")
    }
    func run(_ query: String, stringParams: String) throws -> [NamedRow] {
        let resStr = self.db.run_script_str(query, stringParams, false).toString()
        let dataFromString = resStr.data(using: .utf8, allowLossyConversion: false)!
        let json = JSON(dataFromString);
        if json["ok"].boolValue {
            let jHeaders = json["headers"].arrayValue.map{(j) -> String in
                return j.stringValue
            }
            let headers = RowHeaders(headers: jHeaders)
            return json["rows"].arrayValue.map{(j) -> NamedRow in
                let fields = j.arrayValue
                return NamedRow(headers: headers, fields: fields)
            }
        } else {
            throw CozoError.query(json)
        }
    }
    public func exportRelations(relations: [String]) throws -> JSON {
        let payload = JSON(["relations": relations]).rawString(.utf8, options: .init(rawValue: 0))!
        let resStr = self.db.export_relations_str(payload).toString()
        let dataFromString = resStr.data(using: .utf8, allowLossyConversion: false)!
        let json = JSON(dataFromString);
        if json["ok"].boolValue {
            return json["data"]
        } else {
            throw CozoError.query(json)
        }
    }
    public func importRelations(data: JSON) throws {
        let payload = data.rawString(.utf8, options: .init(rawValue: 0))!
        let resStr = self.db.import_relations_str(payload).toString()
        let dataFromString = resStr.data(using: .utf8, allowLossyConversion: false)!
        let json = JSON(dataFromString);
        if !json["ok"].boolValue {
            throw CozoError.query(json)
        }
    }
    public func backup(path: String) throws {
        let resStr = self.db.backup_db_str(path).toString()
        let dataFromString = resStr.data(using: .utf8, allowLossyConversion: false)!
        let json = JSON(dataFromString);

        if !json["ok"].boolValue {
            throw CozoError.query(json)
        }
    }
    public func restore(path: String) throws {
        let resStr = self.db.restore_backup_str(path).toString()
        let dataFromString = resStr.data(using: .utf8, allowLossyConversion: false)!
        let json = JSON(dataFromString);

        if !json["ok"].boolValue {
            throw CozoError.query(json)
        }
    }
    public func importRelationsFromBackup(path: String, relations: [String]) throws {
        let payload = JSON(["relations": relations, "path": path]).rawString(.utf8, options: .init(rawValue: 0))!
        let resStr = self.db.import_relations_str(payload).toString()
        let dataFromString = resStr.data(using: .utf8, allowLossyConversion: false)!
        let json = JSON(dataFromString);
        if !json["ok"].boolValue {
            throw CozoError.query(json)
        }
    }
}
