import Foundation

extension YFScreenerQuery {
    public func validate(for quoteType: YFScreenerQuoteType) throws {
        let allowedFields: Set<String>
        let allowedValues: [String: Set<String>]
        switch quoteType {
        case .equity:
            allowedFields = YFScreenerConst.equityFields
                .subtracting(YFScreenerConst.equityFieldRemovals)
                .union(YFScreenerConst.equityFieldAdditions)
            allowedValues = Self.mergingAllowedValues(
                YFScreenerConst.equityValidValues,
                with: YFScreenerConst.equityValidValueAdditions
            )
        case .mutualFund:
            allowedFields = YFScreenerConst.fundFields
            allowedValues = YFScreenerConst.fundValidValues
        }

        try validateInternal(allowedFields: allowedFields, allowedValues: allowedValues)
    }

    /// Validates against the ETFQuery field/value set added by Python yfinance 1.3.0.
    public func validateETF() throws {
        try validateInternal(
            allowedFields: YFScreenerConst.etfFields,
            allowedValues: YFScreenerConst.etfValidValues
        )
    }

    private static func mergingAllowedValues(
        _ base: [String: Set<String>],
        with additions: [String: Set<String>]
    ) -> [String: Set<String>] {
        var merged = base
        for (key, values) in additions {
            merged[key, default: []].formUnion(values)
        }
        return merged
    }

    private func validateInternal(allowedFields: Set<String>, allowedValues: [String: Set<String>]) throws {
        let op = `operator`.uppercased()

        switch op {
        case "AND", "OR":
            guard operands.count > 1 else {
                throw YFinanceError.invalidRequest("Operand must contain 2+ queries for \(op)")
            }
            for operand in operands {
                guard case .query(let query) = operand else {
                    throw YFinanceError.invalidRequest("Operand must be query type for \(op)")
                }
                try query.validateInternal(allowedFields: allowedFields, allowedValues: allowedValues)
            }
        case "EQ":
            guard operands.count == 2 else {
                throw YFinanceError.invalidRequest("Operand must contain exactly 2 items for EQ")
            }
            let field = try validateFieldOperand(operands[0], allowedFields: allowedFields)
            try validateAllowedValueIfNeeded(field: field, value: operands[1], allowedValues: allowedValues)
        case "IS-IN":
            guard operands.count >= 2 else {
                throw YFinanceError.invalidRequest("Operand must contain 2+ items for IS-IN")
            }
            let field = try validateFieldOperand(operands[0], allowedFields: allowedFields)
            for operand in operands.dropFirst() {
                try validateAllowedValueIfNeeded(field: field, value: operand, allowedValues: allowedValues)
            }
        case "BTWN":
            guard operands.count == 3 else {
                throw YFinanceError.invalidRequest("Operand must contain exactly 3 items for BTWN")
            }
            _ = try validateFieldOperand(operands[0], allowedFields: allowedFields)
            guard numericValue(from: operands[1]) != nil, numericValue(from: operands[2]) != nil else {
                throw YFinanceError.invalidRequest("BTWN requires numeric bounds")
            }
        case "GT", "LT", "GTE", "LTE":
            guard operands.count == 2 else {
                throw YFinanceError.invalidRequest("Operand must contain exactly 2 items for \(op)")
            }
            _ = try validateFieldOperand(operands[0], allowedFields: allowedFields)
            guard numericValue(from: operands[1]) != nil else {
                throw YFinanceError.invalidRequest("\(op) requires a numeric comparison value")
            }
        default:
            throw YFinanceError.invalidRequest("Invalid screener operator '\(op)'")
        }
    }

    private func validateFieldOperand(_ operand: YFScreenerOperand, allowedFields: Set<String>) throws -> String {
        guard case .string(let rawField) = operand else {
            throw YFinanceError.invalidRequest("Screener field operand must be a string")
        }
        let field = rawField.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !field.isEmpty else {
            throw YFinanceError.invalidRequest("Screener field cannot be empty")
        }
        guard allowedFields.contains(field) else {
            throw YFinanceError.invalidRequest("Invalid screener field '\(rawField)'")
        }
        return field
    }

    private func validateAllowedValueIfNeeded(
        field: String,
        value: YFScreenerOperand,
        allowedValues: [String: Set<String>]
    ) throws {
        guard let allowed = allowedValues[field] else {
            return
        }

        guard case .string(let rawValue) = value else {
            throw YFinanceError.invalidRequest("Invalid EQ value type for '\(field)'")
        }
        guard allowed.contains(rawValue) else {
            throw YFinanceError.invalidRequest("Invalid EQ value '\(rawValue)'")
        }
    }

    private func numericValue(from operand: YFScreenerOperand) -> Double? {
        switch operand {
        case .double(let value):
            return value
        case .int(let value):
            return Double(value)
        default:
            return nil
        }
    }
}
