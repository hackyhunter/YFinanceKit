// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "YFinanceKit",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "YFinanceKit",
            targets: ["YFinanceKit"]
        ),
        .executable(
            name: "YFParityCLI",
            targets: ["YFParityCLI"]
        ),
    ],
    targets: [
        .target(
            name: "YFinanceKit"
        ),
        .executableTarget(
            name: "YFParityCLI",
            dependencies: ["YFinanceKit"]
        ),
        .testTarget(
            name: "YFinanceKitTests",
            dependencies: ["YFinanceKit"]
        ),
    ]
)
