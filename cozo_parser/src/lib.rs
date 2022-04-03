extern crate pest;
#[macro_use]
extern crate pest_derive;

pub mod value;

use pest::Parser;


#[derive(Parser)]
#[grammar = "cozo.pest"]
pub struct CozoParser;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers() {
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x").unwrap().as_str(), "x");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x2").unwrap().as_str(), "x2");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x_y").unwrap().as_str(), "x_y");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x_").unwrap().as_str(), "x_");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "你好").unwrap().as_str(), "你好");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "你好123").unwrap().as_str(), "你好123");
        assert_ne!(CozoParser::parse(Rule::ident, "x$y").unwrap().as_str(), "x$y");

        assert!(CozoParser::parse(Rule::normal_ident, "_x").is_err());
        assert!(CozoParser::parse(Rule::normal_ident, "_").is_err());
        assert_eq!(CozoParser::parse(Rule::ident, "_x").unwrap().as_str(), "_x");
        assert_eq!(CozoParser::parse(Rule::ident, "_").unwrap().as_str(), "_");

        assert!(CozoParser::parse(Rule::normal_ident, "$x").is_err());
        assert!(CozoParser::parse(Rule::ident, "$").is_err());
        assert_eq!(CozoParser::parse(Rule::ident, "$x").unwrap().as_str(), "$x");

        assert!(CozoParser::parse(Rule::ident, "123x").is_err());
        assert!(CozoParser::parse(Rule::ident, ".x").is_err());
        assert_ne!(CozoParser::parse(Rule::ident, "x.x").unwrap().as_str(), "x.x");
        assert_ne!(CozoParser::parse(Rule::ident, "x~x").unwrap().as_str(), "x~x");
    }

    #[test]
    fn strings() {
        assert_eq!(CozoParser::parse(Rule::string, r#""""#).unwrap().as_str(), r#""""#);
        assert_eq!(CozoParser::parse(Rule::string, r#"" b a c""#).unwrap().as_str(), r#"" b a c""#);
        assert_eq!(CozoParser::parse(Rule::string, r#""你好👋""#).unwrap().as_str(), r#""你好👋""#);
        assert_eq!(CozoParser::parse(Rule::string, r#""\n""#).unwrap().as_str(), r#""\n""#);
        assert_eq!(CozoParser::parse(Rule::string, r#""\u5678""#).unwrap().as_str(), r#""\u5678""#);
        assert!(CozoParser::parse(Rule::string, r#""\ux""#).is_err());
        assert_eq!(CozoParser::parse(Rule::string, r###"r#"a"#"###).unwrap().as_str(), r##"r#"a"#"##);
    }

    #[test]
    fn numbers() {
        assert_eq!(CozoParser::parse(Rule::number, "123").unwrap().as_str(), "123");
        assert_eq!(CozoParser::parse(Rule::number, "-123").unwrap().as_str(), "-123");
        assert_eq!(CozoParser::parse(Rule::number, "0").unwrap().as_str(), "0");
        assert_eq!(CozoParser::parse(Rule::number, "-0").unwrap().as_str(), "-0");
        assert_eq!(CozoParser::parse(Rule::number, "0123").unwrap().as_str(), "0123");
        assert_eq!(CozoParser::parse(Rule::number, "000_1").unwrap().as_str(), "000_1");
        assert!(CozoParser::parse(Rule::number, "_000_1").is_err());
        assert_eq!(CozoParser::parse(Rule::number, "0xAf03").unwrap().as_str(), "0xAf03");
        assert_eq!(CozoParser::parse(Rule::number, "0o0_7067").unwrap().as_str(), "0o0_7067");
        assert_ne!(CozoParser::parse(Rule::number, "0o0_7068").unwrap().as_str(), "0o0_7068");
        assert_eq!(CozoParser::parse(Rule::number, "0b0000_0000_1111").unwrap().as_str(), "0b0000_0000_1111");
        assert_ne!(CozoParser::parse(Rule::number, "0b0000_0000_1112").unwrap().as_str(), "0b0000_0000_1112");

        assert_eq!(CozoParser::parse(Rule::number, "123.45").unwrap().as_str(), "123.45");
        assert_eq!(CozoParser::parse(Rule::number, "1_23.4_5_").unwrap().as_str(), "1_23.4_5_");
        assert_ne!(CozoParser::parse(Rule::number, "123.").unwrap().as_str(), "123.");
        assert_eq!(CozoParser::parse(Rule::number, "-123e-456").unwrap().as_str(), "-123e-456");
        assert_eq!(CozoParser::parse(Rule::number, "123.333e456").unwrap().as_str(), "123.333e456");
        assert_eq!(CozoParser::parse(Rule::number, "1_23.33_3e45_6").unwrap().as_str(), "1_23.33_3e45_6");
    }

    #[test]
    fn expressions() {
        assert!(CozoParser::parse(Rule::expr, r"(a + b) ~ [] + c.d.e(1,2,x=3).f").is_ok());
    }
}
