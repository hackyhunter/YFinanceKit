import Foundation

// Python yfinance "MIC -> Yahoo suffix" compatibility.
// Source: yfinance/const.py (_MIC_TO_YAHOO_SUFFIX)

internal let _MIC_TO_YAHOO_SUFFIX: [String: String] = [
    "XCBT": "CBT",
    "XCME": "CME",
    "IFUS": "NYB",
    "CECS": "CMX",
    "XNYM": "NYM",
    "XNYS": "",
    "XNAS": "",
    "XBUE": "BA",
    "XVIE": "VI",
    "XASX": "AX",
    "XAUS": "XA",
    "XBRU": "BR",
    "BVMF": "SA",
    "CNSX": "CN",
    "NEOE": "NE",
    "XTSE": "TO",
    "XTSX": "V",
    "XSGO": "SN",
    "XSHG": "SS",
    "XSHE": "SZ",
    "XBOG": "CL",
    "XPRA": "PR",
    "XCSE": "CO",
    "XCAI": "CA",
    "XTAL": "TL",
    "CEUX": "XD",
    "XEUR": "NX",
    "XHEL": "HE",
    "XPAR": "PA",
    "XBER": "BE",
    "XBMS": "BM",
    "XDUS": "DU",
    "XFRA": "F",
    "XHAM": "HM",
    "XHAN": "HA",
    "XMUN": "MU",
    "XSTU": "SG",
    "XETR": "DE",
    "XATH": "AT",
    "XHKG": "HK",
    "XBUD": "BD",
    "XICE": "IC",
    "XBOM": "BO",
    "XNSE": "NS",
    "XIDX": "JK",
    "XDUB": "IR",
    "XTAE": "TA",
    "MTAA": "MI",
    "EUTL": "TI",
    "XTKS": "T",
    "XKFE": "KW",
    "XRIS": "RG",
    "XVIL": "VS",
    "XKLS": "KL",
    "XMEX": "MX",
    "XAMS": "AS",
    "XNZE": "NZ",
    "XOSL": "OL",
    "XPHS": "PS",
    "XWAR": "WA",
    "XLIS": "LS",
    "XQAT": "QA",
    "XBSE": "RO",
    "XSES": "SI",
    "XJSE": "JO",
    "XKRX": "KS",
    "KQKS": "KQ",
    "BMEX": "MC",
    "XSAU": "SR",
    "XSTO": "ST",
    "XSWX": "SW",
    "ROCO": "TWO",
    "XTAI": "TW",
    "XBKK": "BK",
    "XIST": "IS",
    "XDFM": "AE",
    "AQXE": "AQ",
    "XCHI": "XC",
    "XLON": "L",
    "ILSE": "IL",
    "XCAR": "CR",
    "XSTC": "VN",
]

public func yahooTicker(baseSymbol: String, mic: String) throws -> String {
    let cleanedBase = baseSymbol.trimmingCharacters(in: .whitespacesAndNewlines)
    var micCode = mic.trimmingCharacters(in: .whitespacesAndNewlines)
    if micCode.hasPrefix(".") {
        micCode.removeFirst()
    }
    let upperMic = micCode.uppercased()
    guard let suffix = _MIC_TO_YAHOO_SUFFIX[upperMic] else {
        throw YFinanceError.invalidRequest("Unknown MIC code: '\(micCode)'")
    }
    if suffix.isEmpty {
        return cleanedBase
    }
    return "\(cleanedBase).\(suffix)"
}

public func yahoo_ticker(_ base_symbol: String, mic_code: String) throws -> String {
    try yahooTicker(baseSymbol: base_symbol, mic: mic_code)
}

