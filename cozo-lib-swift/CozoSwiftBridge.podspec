Pod::Spec.new do |spec|
  spec.name         = "CozoSwiftBridge"
  spec.version      = "0.7.6"
  spec.summary      = "CozoDB for Swift"
  spec.description  = "This library allows you to use CozoDB embedded in your Swift application"
  spec.homepage     = "https://github.com/cozodb/cozo/"
  spec.license      = "MPL-2.0"
  spec.author       = { "Ziyang Hu" => "hu.ziyang@cantab.net" }
  spec.source       = { :http => "https://github.com/cozodb/cozo/releases/download/v0.7.6/CozoSwiftBridge-0.7.6.tgz" }
  spec.source_files = "Sources/CozoSwiftBridge/*"
  spec.vendored_frameworks = "RustXcframework.xcframework"
  spec.requires_arc = true
  spec.swift_version = "5.0"
  spec.osx.deployment_target = "10.13"
  spec.ios.deployment_target = "11.0"
  spec.dependency "SwiftyJSON", "~> 4.0"
end
