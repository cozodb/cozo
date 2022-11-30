// swift-tools-version:5.5.0
import PackageDescription
let package = Package(
	name: "CozoSwiftBridge",
	products: [
		.library(
			name: "CozoSwiftBridge",
			targets: ["CozoSwiftBridge"]),
	],
	dependencies: [
        .package(url: "https://github.com/SwiftyJSON/SwiftyJSON.git", from: "4.0.0"),
	],
	targets: [
		.binaryTarget(
			name: "RustXcframework",
			path: "RustXcframework.xcframework"
		),
		.target(
			name: "CozoSwiftBridge",
			dependencies: ["RustXcframework", "SwiftyJSON"])
	]
)
	