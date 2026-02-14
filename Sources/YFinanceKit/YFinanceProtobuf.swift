import Foundation

public enum YFStreamQuoteType: Sendable, Equatable {
    case none
    case altSymbol
    case heartbeat
    case equity
    case index
    case mutualFund
    case moneyMarket
    case etf
    case currency
    case option
    case warrant
    case bond
    case future
    case commodity
    case ecnQuote
    case cryptocurrency
    case indicator
    case culIdx
    case culSubIdx
    case culAsset
    case privateCompany
    case industry
    case unknown(Int32)

    public init(rawValue: Int32) {
        switch rawValue {
        case 0:
            self = .none
        case 5:
            self = .altSymbol
        case 7:
            self = .heartbeat
        case 8:
            self = .equity
        case 9:
            self = .index
        case 11:
            self = .mutualFund
        case 12:
            self = .moneyMarket
        case 13:
            self = .option
        case 14:
            self = .currency
        case 15:
            self = .warrant
        case 17:
            self = .bond
        case 18:
            self = .future
        case 20:
            self = .etf
        case 23:
            self = .commodity
        case 28:
            self = .ecnQuote
        case 41:
            self = .cryptocurrency
        case 42:
            self = .indicator
        case 43:
            self = .culIdx
        case 44:
            self = .culSubIdx
        case 45:
            self = .culAsset
        case 46:
            self = .privateCompany
        case 1_000:
            self = .industry
        default:
            self = .unknown(rawValue)
        }
    }

    public var rawValue: Int32 {
        switch self {
        case .none:
            return 0
        case .altSymbol:
            return 5
        case .heartbeat:
            return 7
        case .equity:
            return 8
        case .index:
            return 9
        case .mutualFund:
            return 11
        case .moneyMarket:
            return 12
        case .option:
            return 13
        case .currency:
            return 14
        case .warrant:
            return 15
        case .bond:
            return 17
        case .future:
            return 18
        case .etf:
            return 20
        case .commodity:
            return 23
        case .ecnQuote:
            return 28
        case .cryptocurrency:
            return 41
        case .indicator:
            return 42
        case .culIdx:
            return 43
        case .culSubIdx:
            return 44
        case .culAsset:
            return 45
        case .privateCompany:
            return 46
        case .industry:
            return 1_000
        case .unknown(let raw):
            return raw
        }
    }
}

public enum YFStreamMarketHours: Sendable, Equatable {
    case preMarket
    case regularMarket
    case postMarket
    case extendedHoursMarket
    case overnightMarket
    case unknown(Int32)

    public init(rawValue: Int32) {
        switch rawValue {
        case 0:
            self = .preMarket
        case 1:
            self = .regularMarket
        case 2:
            self = .postMarket
        case 3:
            self = .extendedHoursMarket
        case 4:
            self = .overnightMarket
        default:
            self = .unknown(rawValue)
        }
    }

    public var rawValue: Int32 {
        switch self {
        case .preMarket:
            return 0
        case .regularMarket:
            return 1
        case .postMarket:
            return 2
        case .extendedHoursMarket:
            return 3
        case .overnightMarket:
            return 4
        case .unknown(let raw):
            return raw
        }
    }
}

public struct YFPricingData: Sendable, Equatable {
    public var id: String?
    public var price: Float?
    public var time: Int64?
    public var currency: String?
    public var exchange: String?
    public var quoteType: Int32?
    public var marketHours: Int32?
    public var changePercent: Float?
    public var dayVolume: Int64?
    public var dayHigh: Float?
    public var dayLow: Float?
    public var change: Float?
    public var shortName: String?
    public var expireDate: Int64?
    public var openPrice: Float?
    public var previousClose: Float?
    public var strikePrice: Float?
    public var underlyingSymbol: String?
    public var openInterest: Int64?
    public var optionsType: Int64?
    public var miniOption: Int64?
    public var lastSize: Int64?
    public var bid: Float?
    public var bidSize: Int64?
    public var ask: Float?
    public var askSize: Int64?
    public var priceHint: Int64?
    public var vol24Hr: Int64?
    public var volAllCurrencies: Int64?
    public var fromCurrency: String?
    public var lastMarket: String?
    public var circulatingSupply: Double?
    public var marketCap: Double?

    public init() {}

    public var symbol: String? { id }
    public var quoteTypeValue: YFStreamQuoteType? { quoteType.map { YFStreamQuoteType(rawValue: $0) } }
    public var marketHoursValue: YFStreamMarketHours? { marketHours.map { YFStreamMarketHours(rawValue: $0) } }
}

enum YFProtobufDecodeError: Error {
    case truncated
    case malformedVarint
    case unsupportedWireType(Int)
}

struct YFProtobufDecoder {
    static func decodePricingData(_ data: Data) throws -> YFPricingData {
        var state = DecoderState(data: data)
        var output = YFPricingData()

        while !state.isAtEnd {
            let key = try state.readVarint()
            let fieldNumber = Int(key >> 3)
            let wireType = Int(key & 0x7)

            switch fieldNumber {
            case 1:
                output.id = try state.readString(wireType: wireType)
            case 2:
                output.price = try state.readFloat(wireType: wireType)
            case 3:
                output.time = Int64(try state.readSInt64(wireType: wireType))
            case 4:
                output.currency = try state.readString(wireType: wireType)
            case 5:
                output.exchange = try state.readString(wireType: wireType)
            case 6:
                output.quoteType = Int32(truncatingIfNeeded: try state.readInt64(wireType: wireType))
            case 7:
                output.marketHours = Int32(truncatingIfNeeded: try state.readInt64(wireType: wireType))
            case 8:
                output.changePercent = try state.readFloat(wireType: wireType)
            case 9:
                output.dayVolume = Int64(try state.readSInt64(wireType: wireType))
            case 10:
                output.dayHigh = try state.readFloat(wireType: wireType)
            case 11:
                output.dayLow = try state.readFloat(wireType: wireType)
            case 12:
                output.change = try state.readFloat(wireType: wireType)
            case 13:
                output.shortName = try state.readString(wireType: wireType)
            case 14:
                output.expireDate = Int64(try state.readSInt64(wireType: wireType))
            case 15:
                output.openPrice = try state.readFloat(wireType: wireType)
            case 16:
                output.previousClose = try state.readFloat(wireType: wireType)
            case 17:
                output.strikePrice = try state.readFloat(wireType: wireType)
            case 18:
                output.underlyingSymbol = try state.readString(wireType: wireType)
            case 19:
                output.openInterest = Int64(try state.readSInt64(wireType: wireType))
            case 20:
                output.optionsType = Int64(try state.readSInt64(wireType: wireType))
            case 21:
                output.miniOption = Int64(try state.readSInt64(wireType: wireType))
            case 22:
                output.lastSize = Int64(try state.readSInt64(wireType: wireType))
            case 23:
                output.bid = try state.readFloat(wireType: wireType)
            case 24:
                output.bidSize = Int64(try state.readSInt64(wireType: wireType))
            case 25:
                output.ask = try state.readFloat(wireType: wireType)
            case 26:
                output.askSize = Int64(try state.readSInt64(wireType: wireType))
            case 27:
                output.priceHint = Int64(try state.readSInt64(wireType: wireType))
            case 28:
                output.vol24Hr = Int64(try state.readSInt64(wireType: wireType))
            case 29:
                output.volAllCurrencies = Int64(try state.readSInt64(wireType: wireType))
            case 30:
                output.fromCurrency = try state.readString(wireType: wireType)
            case 31:
                output.lastMarket = try state.readString(wireType: wireType)
            case 32:
                output.circulatingSupply = try state.readDouble(wireType: wireType)
            case 33:
                output.marketCap = try state.readDouble(wireType: wireType)
            default:
                try state.skipField(wireType: wireType)
            }
        }

        return output
    }
}

private struct DecoderState {
    let data: Data
    var offset: Int = 0

    var isAtEnd: Bool {
        offset >= data.count
    }

    mutating func readVarint() throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0
        var index = 0

        while true {
            guard offset < data.count else {
                throw YFProtobufDecodeError.truncated
            }
            let byte = data[offset]
            offset += 1

            result |= UInt64(byte & 0x7F) << shift
            if (byte & 0x80) == 0 {
                return result
            }

            shift += 7
            index += 1
            if index > 9 {
                throw YFProtobufDecodeError.malformedVarint
            }
        }
    }

    mutating func readString(wireType: Int) throws -> String {
        guard wireType == 2 else {
            try skipField(wireType: wireType)
            return ""
        }
        let bytes = try readLengthDelimited()
        return String(decoding: bytes, as: UTF8.self)
    }

    mutating func readFloat(wireType: Int) throws -> Float {
        guard wireType == 5 else {
            try skipField(wireType: wireType)
            return .nan
        }
        let bitPattern = try readFixed32()
        return Float(bitPattern: bitPattern)
    }

    mutating func readDouble(wireType: Int) throws -> Double {
        guard wireType == 1 else {
            try skipField(wireType: wireType)
            return .nan
        }
        let bitPattern = try readFixed64()
        return Double(bitPattern: bitPattern)
    }

    mutating func readInt64(wireType: Int) throws -> Int64 {
        guard wireType == 0 else {
            try skipField(wireType: wireType)
            return 0
        }
        return Int64(bitPattern: try readVarint())
    }

    mutating func readSInt64(wireType: Int) throws -> Int64 {
        guard wireType == 0 else {
            try skipField(wireType: wireType)
            return 0
        }
        let encoded = try readVarint()
        return zigZagDecode(encoded)
    }

    mutating func skipField(wireType: Int) throws {
        switch wireType {
        case 0:
            _ = try readVarint()
        case 1:
            try skipBytes(8)
        case 2:
            let length = try Int(readVarint())
            try skipBytes(length)
        case 5:
            try skipBytes(4)
        default:
            throw YFProtobufDecodeError.unsupportedWireType(wireType)
        }
    }

    private mutating func readLengthDelimited() throws -> Data {
        let length = try Int(readVarint())
        guard offset + length <= data.count else {
            throw YFProtobufDecodeError.truncated
        }
        let subdata = data.subdata(in: offset ..< (offset + length))
        offset += length
        return subdata
    }

    private mutating func readFixed32() throws -> UInt32 {
        guard offset + 4 <= data.count else {
            throw YFProtobufDecodeError.truncated
        }
        let b0 = UInt32(data[offset])
        let b1 = UInt32(data[offset + 1]) << 8
        let b2 = UInt32(data[offset + 2]) << 16
        let b3 = UInt32(data[offset + 3]) << 24
        offset += 4
        return b0 | b1 | b2 | b3
    }

    private mutating func readFixed64() throws -> UInt64 {
        guard offset + 8 <= data.count else {
            throw YFProtobufDecodeError.truncated
        }
        var result: UInt64 = 0
        for i in 0..<8 {
            result |= UInt64(data[offset + i]) << UInt64(i * 8)
        }
        offset += 8
        return result
    }

    private mutating func skipBytes(_ count: Int) throws {
        guard count >= 0, offset + count <= data.count else {
            throw YFProtobufDecodeError.truncated
        }
        offset += count
    }

    private func zigZagDecode(_ value: UInt64) -> Int64 {
        let shifted = Int64(value >> 1)
        let sign = Int64(value & 1)
        return shifted ^ -sign
    }
}
