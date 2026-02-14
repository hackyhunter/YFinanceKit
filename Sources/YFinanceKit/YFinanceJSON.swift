import Foundation

public enum YFJSONValue: Sendable, Equatable {
    case null
    case bool(Bool)
    case number(Double)
    case string(String)
    case array([YFJSONValue])
    case object([String: YFJSONValue])

    public init(any value: Any) throws {
        switch value {
        case is NSNull:
            self = .null
        case let value as Bool:
            self = .bool(value)
        case let value as NSNumber:
            // NSNumber can represent booleans too, handle those first.
            if String(cString: value.objCType) == "c" {
                self = .bool(value.boolValue)
            } else {
                self = .number(value.doubleValue)
            }
        case let value as String:
            self = .string(value)
        case let value as [Any]:
            self = .array(try value.map { try YFJSONValue(any: $0) })
        case let value as [String: Any]:
            var object: [String: YFJSONValue] = [:]
            object.reserveCapacity(value.count)
            for (key, value) in value {
                object[key] = try YFJSONValue(any: value)
            }
            self = .object(object)
        default:
            throw YFinanceError.invalidRequest("Unsupported JSON value type: \(type(of: value))")
        }
    }

    public var objectValue: [String: YFJSONValue]? {
        if case .object(let value) = self { return value }
        return nil
    }

    public var arrayValue: [YFJSONValue]? {
        if case .array(let value) = self { return value }
        return nil
    }

    public var stringValue: String? {
        if case .string(let value) = self { return value }
        return nil
    }

    public var boolValue: Bool? {
        if case .bool(let value) = self { return value }
        return nil
    }

    public var doubleValue: Double? {
        if case .number(let value) = self { return value }
        return nil
    }

    public var intValue: Int? {
        guard let doubleValue else { return nil }
        return Int(doubleValue)
    }

    public subscript(key: String) -> YFJSONValue? {
        objectValue?[key]
    }

    public subscript(index: Int) -> YFJSONValue? {
        guard let arrayValue, arrayValue.indices.contains(index) else {
            return nil
        }
        return arrayValue[index]
    }

    public func value(at path: [String]) -> YFJSONValue? {
        var current: YFJSONValue? = self
        for key in path {
            current = current?[key]
            if current == nil {
                return nil
            }
        }
        return current
    }

    public func toTable() -> YFTable {
        switch self {
        case .array(let values):
            return YFTable.fromObjects(values)
        case .object(let object):
            return YFTable(columns: Array(object.keys), rows: [object])
        default:
            return YFTable(columns: ["value"], rows: [["value": self]])
        }
    }

    func toFoundationObject() -> Any {
        switch self {
        case .null:
            return NSNull()
        case .bool(let value):
            return value
        case .number(let value):
            return value
        case .string(let value):
            return value
        case .array(let array):
            return array.map { $0.toFoundationObject() }
        case .object(let object):
            var output: [String: Any] = [:]
            output.reserveCapacity(object.count)
            for (key, value) in object {
                output[key] = value.toFoundationObject()
            }
            return output
        }
    }

    static func decode(data: Data) throws -> YFJSONValue {
        let any = try JSONSerialization.jsonObject(with: data, options: [])
        return try YFJSONValue(any: any)
    }

    static func encode(_ value: YFJSONValue) throws -> Data {
        try JSONSerialization.data(withJSONObject: value.toFoundationObject(), options: [])
    }
}

extension Dictionary where Key == String, Value == YFJSONValue {
    func value(at path: [String]) -> YFJSONValue? {
        YFJSONValue.object(self).value(at: path)
    }
}
