use pest_derive::Parser;


#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct Parser;


#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser as PestParser;

    #[test]
    fn db() {
        use rocksdb::{DB, Options};
// NB: db is automatically closed at end of lifetime
        let path = "_path_for_rocksdb_storage";
        {
            let db = DB::open_default(path).unwrap();
            db.put("真二", "你好👋").unwrap();
            match db.get_pinned("真二") {
                Ok(Some(value)) => println!("retrieved value {}", std::str::from_utf8(&value).unwrap()),
                Ok(None) => println!("value not found"),
                Err(e) => println!("operational problem encountered: {}", e),
            }
            db.delete(b"my key").unwrap();
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn identifiers() {
        assert_eq!(Parser::parse(Rule::ident, "x").unwrap().as_str(), "x");
        assert_eq!(Parser::parse(Rule::ident, "x2").unwrap().as_str(), "x2");
        assert_eq!(Parser::parse(Rule::ident, "x_y").unwrap().as_str(), "x_y");
        assert_eq!(Parser::parse(Rule::ident, "x_").unwrap().as_str(), "x_");
        assert_eq!(Parser::parse(Rule::ident, "你好").unwrap().as_str(), "你好");
        assert_eq!(Parser::parse(Rule::ident, "你好123").unwrap().as_str(), "你好123");
        assert_ne!(Parser::parse(Rule::ident, "x$y").unwrap().as_str(), "x$y");

        assert_eq!(Parser::parse(Rule::ident, "_x").unwrap().as_str(), "_x");
        assert_eq!(Parser::parse(Rule::ident, "_").unwrap().as_str(), "_");

        assert!(Parser::parse(Rule::ident, "$x").is_err());
        assert!(Parser::parse(Rule::ident, "$").is_err());
        assert_eq!(Parser::parse(Rule::param, "$x").unwrap().as_str(), "$x");

        assert!(Parser::parse(Rule::ident, "123x").is_err());
        assert!(Parser::parse(Rule::ident, ".x").is_err());
        assert_ne!(Parser::parse(Rule::ident, "x.x").unwrap().as_str(), "x.x");
        assert_ne!(Parser::parse(Rule::ident, "x~x").unwrap().as_str(), "x~x");
    }

    #[test]
    fn strings() {
        assert_eq!(Parser::parse(Rule::string, r#""""#).unwrap().as_str(), r#""""#);
        assert_eq!(Parser::parse(Rule::string, r#"" b a c""#).unwrap().as_str(), r#"" b a c""#);
        assert_eq!(Parser::parse(Rule::string, r#""你好👋""#).unwrap().as_str(), r#""你好👋""#);
        assert_eq!(Parser::parse(Rule::string, r#""\n""#).unwrap().as_str(), r#""\n""#);
        assert_eq!(Parser::parse(Rule::string, r#""\u5678""#).unwrap().as_str(), r#""\u5678""#);
        assert!(Parser::parse(Rule::string, r#""\ux""#).is_err());
        assert_eq!(Parser::parse(Rule::string, r###"r#"a"#"###).unwrap().as_str(), r##"r#"a"#"##);
    }

    #[test]
    fn numbers() {
        assert_eq!(Parser::parse(Rule::number, "123").unwrap().as_str(), "123");
        assert_eq!(Parser::parse(Rule::number, "0").unwrap().as_str(), "0");
        assert_eq!(Parser::parse(Rule::number, "0123").unwrap().as_str(), "0123");
        assert_eq!(Parser::parse(Rule::number, "000_1").unwrap().as_str(), "000_1");
        assert!(Parser::parse(Rule::number, "_000_1").is_err());
        assert_eq!(Parser::parse(Rule::number, "0xAf03").unwrap().as_str(), "0xAf03");
        assert_eq!(Parser::parse(Rule::number, "0o0_7067").unwrap().as_str(), "0o0_7067");
        assert_ne!(Parser::parse(Rule::number, "0o0_7068").unwrap().as_str(), "0o0_7068");
        assert_eq!(Parser::parse(Rule::number, "0b0000_0000_1111").unwrap().as_str(), "0b0000_0000_1111");
        assert_ne!(Parser::parse(Rule::number, "0b0000_0000_1112").unwrap().as_str(), "0b0000_0000_1112");

        assert_eq!(Parser::parse(Rule::number, "123.45").unwrap().as_str(), "123.45");
        assert_eq!(Parser::parse(Rule::number, "1_23.4_5_").unwrap().as_str(), "1_23.4_5_");
        assert_ne!(Parser::parse(Rule::number, "123.").unwrap().as_str(), "123.");
        assert_eq!(Parser::parse(Rule::number, "123.333e456").unwrap().as_str(), "123.333e456");
        assert_eq!(Parser::parse(Rule::number, "1_23.33_3e45_6").unwrap().as_str(), "1_23.33_3e45_6");
    }

    #[test]
    fn expressions() {
        assert!(Parser::parse(Rule::expr, r"(a + b) ~ [] + c.d.e(1,2,x=3).f").is_ok());
        // print!("{:#?}", CozoParser::parse(Rule::expr, r"(a + b) ~ [] + c.d.e(1,2,x=3).f"));
    }
}
