//! Expression parser and evaluator for filter expressions.
//!
//! Supports expressions like:
//! - `score > 90`
//! - `name == 'Alice'`
//! - `score > 90 AND name != 'Bob'`
//! - `(age >= 18) OR (has_permission == true)`
//! - `value IS NULL`
//! - `value IS NOT NULL`

use crate::column::ColumnValue;
use std::collections::HashMap;

/// A parsed expression that can be evaluated against a row.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Compare column to a literal value
    Compare {
        column: String,
        op: CompareOp,
        value: LiteralValue,
    },
    /// Check if column is NULL
    IsNull { column: String },
    /// Check if column is NOT NULL
    IsNotNull { column: String },
    /// Logical AND of two expressions
    And(Box<Expr>, Box<Expr>),
    /// Logical OR of two expressions
    Or(Box<Expr>, Box<Expr>),
    /// Logical NOT of an expression
    Not(Box<Expr>),
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,      // ==
    Ne,      // !=
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=
}

/// Literal values that can appear in expressions
#[derive(Debug, Clone)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

/// Token types for lexing
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
    // Operators
    Eq,       // ==
    Ne,       // !=
    Lt,       // <
    Le,       // <=
    Gt,       // >
    Ge,       // >=
    And,
    Or,
    Not,
    Is,
    LParen,
    RParen,
    Eof,
}

/// Lexer for tokenizing expression strings
struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.peek();
        self.pos += 1;
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let mut ident = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                ident.push(c);
                self.advance();
            } else {
                break;
            }
        }
        ident
    }

    fn read_number(&mut self) -> Token {
        let mut num_str = String::new();
        let mut is_float = false;

        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                num_str.push(c);
                self.advance();
            } else if c == '.' && !is_float {
                is_float = true;
                num_str.push(c);
                self.advance();
            } else {
                break;
            }
        }

        if is_float {
            Token::Float(num_str.parse().unwrap_or(0.0))
        } else {
            Token::Int(num_str.parse().unwrap_or(0))
        }
    }

    fn read_string(&mut self, quote: char) -> Result<Token, String> {
        self.advance(); // consume opening quote
        let mut s = String::new();

        while let Some(c) = self.peek() {
            if c == quote {
                self.advance(); // consume closing quote
                return Ok(Token::String(s));
            } else if c == '\\' {
                self.advance();
                if let Some(escaped) = self.advance() {
                    match escaped {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        '\'' => s.push('\''),
                        '"' => s.push('"'),
                        _ => s.push(escaped),
                    }
                }
            } else {
                s.push(c);
                self.advance();
            }
        }

        Err("Unterminated string".to_string())
    }

    fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();

        match self.peek() {
            None => Ok(Token::Eof),
            Some(c) => {
                match c {
                    '(' => { self.advance(); Ok(Token::LParen) }
                    ')' => { self.advance(); Ok(Token::RParen) }
                    '=' => {
                        self.advance();
                        if self.peek() == Some('=') {
                            self.advance();
                            Ok(Token::Eq)
                        } else {
                            Ok(Token::Eq) // Single = also means ==
                        }
                    }
                    '!' => {
                        self.advance();
                        if self.peek() == Some('=') {
                            self.advance();
                            Ok(Token::Ne)
                        } else {
                            Ok(Token::Not)
                        }
                    }
                    '<' => {
                        self.advance();
                        if self.peek() == Some('=') {
                            self.advance();
                            Ok(Token::Le)
                        } else {
                            Ok(Token::Lt)
                        }
                    }
                    '>' => {
                        self.advance();
                        if self.peek() == Some('=') {
                            self.advance();
                            Ok(Token::Ge)
                        } else {
                            Ok(Token::Gt)
                        }
                    }
                    '\'' | '"' => self.read_string(c),
                    '-' if self.input.get(self.pos + 1).map_or(false, |c| c.is_ascii_digit() || *c == '.') => {
                        self.advance(); // consume '-'
                        let token = self.read_number();
                        match token {
                            Token::Int(v) => Ok(Token::Int(-v)),
                            Token::Float(v) => Ok(Token::Float(-v)),
                            other => Ok(other),
                        }
                    }
                    _ if c.is_ascii_digit() => Ok(self.read_number()),
                    _ if c.is_alphabetic() || c == '_' => {
                        let ident = self.read_ident();
                        // Check for keywords
                        match ident.to_uppercase().as_str() {
                            "AND" => Ok(Token::And),
                            "OR" => Ok(Token::Or),
                            "NOT" => Ok(Token::Not),
                            "IS" => Ok(Token::Is),
                            "NULL" => Ok(Token::Null),
                            "TRUE" => Ok(Token::Bool(true)),
                            "FALSE" => Ok(Token::Bool(false)),
                            _ => Ok(Token::Ident(ident)),
                        }
                    }
                    _ => Err(format!("Unexpected character: {}", c)),
                }
            }
        }
    }
}

/// Parser for building expression AST
struct Parser {
    lexer: Lexer,
    current: Token,
}

impl Parser {
    fn new(input: &str) -> Result<Self, String> {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token()?;
        Ok(Parser { lexer, current })
    }

    fn advance(&mut self) -> Result<(), String> {
        self.current = self.lexer.next_token()?;
        Ok(())
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        if &self.current == expected {
            self.advance()
        } else {
            Err(format!("Expected {:?}, got {:?}", expected, self.current))
        }
    }

    /// Parse a full expression
    fn parse(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    /// Parse OR expressions (lowest precedence)
    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;

        while self.current == Token::Or {
            self.advance()?;
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    /// Parse AND expressions
    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not()?;

        while self.current == Token::And {
            self.advance()?;
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    /// Parse NOT expressions
    fn parse_not(&mut self) -> Result<Expr, String> {
        if self.current == Token::Not {
            self.advance()?;
            let expr = self.parse_not()?;
            Ok(Expr::Not(Box::new(expr)))
        } else {
            self.parse_comparison()
        }
    }

    /// Parse comparison expressions
    fn parse_comparison(&mut self) -> Result<Expr, String> {
        // Handle parentheses
        if self.current == Token::LParen {
            self.advance()?;
            let expr = self.parse()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }

        // Expect a column name
        let column = match &self.current {
            Token::Ident(name) => name.clone(),
            _ => return Err(format!("Expected column name, got {:?}", self.current)),
        };
        self.advance()?;

        // Handle IS NULL / IS NOT NULL
        if self.current == Token::Is {
            self.advance()?;
            if self.current == Token::Not {
                self.advance()?;
                if self.current != Token::Null {
                    return Err("Expected NULL after IS NOT".to_string());
                }
                self.advance()?;
                return Ok(Expr::IsNotNull { column });
            } else if self.current == Token::Null {
                self.advance()?;
                return Ok(Expr::IsNull { column });
            } else {
                return Err("Expected NULL or NOT NULL after IS".to_string());
            }
        }

        // Parse comparison operator
        let op = match &self.current {
            Token::Eq => CompareOp::Eq,
            Token::Ne => CompareOp::Ne,
            Token::Lt => CompareOp::Lt,
            Token::Le => CompareOp::Le,
            Token::Gt => CompareOp::Gt,
            Token::Ge => CompareOp::Ge,
            _ => return Err(format!("Expected comparison operator, got {:?}", self.current)),
        };
        self.advance()?;

        // Parse literal value
        let value = match &self.current {
            Token::Int(n) => LiteralValue::Int(*n),
            Token::Float(f) => LiteralValue::Float(*f),
            Token::String(s) => LiteralValue::String(s.clone()),
            Token::Bool(b) => LiteralValue::Bool(*b),
            Token::Null => LiteralValue::Null,
            _ => return Err(format!("Expected literal value, got {:?}", self.current)),
        };
        self.advance()?;

        Ok(Expr::Compare { column, op, value })
    }
}

/// Parse an expression string into an Expr AST.
pub fn parse_expr(input: &str) -> Result<Expr, String> {
    let mut parser = Parser::new(input)?;
    let expr = parser.parse()?;

    // Ensure we consumed all input
    if parser.current != Token::Eof {
        return Err(format!("Unexpected token after expression: {:?}", parser.current));
    }

    Ok(expr)
}

/// Evaluate an expression against a row.
pub fn eval_expr(expr: &Expr, row: &HashMap<String, ColumnValue>) -> bool {
    match expr {
        Expr::Compare { column, op, value } => {
            match row.get(column) {
                None => false, // Column not found
                Some(col_val) => compare_values(col_val, op, value),
            }
        }
        Expr::IsNull { column } => {
            matches!(row.get(column), Some(ColumnValue::Null) | None)
        }
        Expr::IsNotNull { column } => {
            match row.get(column) {
                Some(ColumnValue::Null) | None => false,
                Some(_) => true,
            }
        }
        Expr::And(left, right) => {
            eval_expr(left, row) && eval_expr(right, row)
        }
        Expr::Or(left, right) => {
            eval_expr(left, row) || eval_expr(right, row)
        }
        Expr::Not(inner) => {
            !eval_expr(inner, row)
        }
    }
}

/// Compare a column value to a literal value.
fn compare_values(col_val: &ColumnValue, op: &CompareOp, lit_val: &LiteralValue) -> bool {
    match (col_val, lit_val) {
        // NULL comparisons: any comparison involving NULL yields UNKNOWN (treated as false).
        // Use IS NULL / IS NOT NULL to test for nulls.
        (ColumnValue::Null, _) | (_, LiteralValue::Null) => false,

        // Integer comparisons
        (ColumnValue::Int32(a), LiteralValue::Int(b)) => compare_ord(*a as i64, *b, op),
        (ColumnValue::Int64(a), LiteralValue::Int(b)) => compare_ord(*a, *b, op),
        (ColumnValue::Int32(a), LiteralValue::Float(b)) => compare_ord(*a as f64, *b, op),
        (ColumnValue::Int64(a), LiteralValue::Float(b)) => compare_ord(*a as f64, *b, op),

        // Float comparisons
        (ColumnValue::Float32(a), LiteralValue::Float(b)) => compare_ord(*a as f64, *b, op),
        (ColumnValue::Float64(a), LiteralValue::Float(b)) => compare_ord(*a, *b, op),
        (ColumnValue::Float32(a), LiteralValue::Int(b)) => compare_ord(*a as f64, *b as f64, op),
        (ColumnValue::Float64(a), LiteralValue::Int(b)) => compare_ord(*a, *b as f64, op),

        // String comparisons
        (ColumnValue::String(a), LiteralValue::String(b)) => compare_ord(a.as_str(), b.as_str(), op),

        // Boolean comparisons
        (ColumnValue::Bool(a), LiteralValue::Bool(b)) => {
            match op {
                CompareOp::Eq => a == b,
                CompareOp::Ne => a != b,
                _ => false, // < > <= >= don't make sense for bools
            }
        }

        // Type mismatches return false
        _ => false,
    }
}

/// Compare two ordered values.
fn compare_ord<T: PartialOrd>(a: T, b: T, op: &CompareOp) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

// ============================================================================
// Fast evaluation (zero-allocation) using direct column access
// ============================================================================

/// Evaluate an expression using a column lookup function.
/// This avoids allocating a HashMap per row - the lookup function
/// directly accesses column data.
pub fn eval_expr_fast<F>(expr: &Expr, get_column: &F) -> bool
where
    F: Fn(&str) -> Option<ColumnValue>,
{
    match expr {
        Expr::Compare { column, op, value } => {
            match get_column(column) {
                None => false,
                Some(col_val) => compare_values(&col_val, op, value),
            }
        }
        Expr::IsNull { column } => {
            matches!(get_column(column), Some(ColumnValue::Null) | None)
        }
        Expr::IsNotNull { column } => {
            match get_column(column) {
                Some(ColumnValue::Null) | None => false,
                Some(_) => true,
            }
        }
        Expr::And(left, right) => {
            eval_expr_fast(left, get_column) && eval_expr_fast(right, get_column)
        }
        Expr::Or(left, right) => {
            eval_expr_fast(left, get_column) || eval_expr_fast(right, get_column)
        }
        Expr::Not(inner) => {
            !eval_expr_fast(inner, get_column)
        }
    }
}

/// Extract all column names referenced in an expression.
pub fn extract_columns(expr: &Expr) -> Vec<String> {
    let mut columns = Vec::new();
    extract_columns_recursive(expr, &mut columns);
    columns.sort();
    columns.dedup();
    columns
}

fn extract_columns_recursive(expr: &Expr, columns: &mut Vec<String>) {
    match expr {
        Expr::Compare { column, .. } => columns.push(column.clone()),
        Expr::IsNull { column } => columns.push(column.clone()),
        Expr::IsNotNull { column } => columns.push(column.clone()),
        Expr::And(left, right) | Expr::Or(left, right) => {
            extract_columns_recursive(left, columns);
            extract_columns_recursive(right, columns);
        }
        Expr::Not(inner) => extract_columns_recursive(inner, columns),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row() -> HashMap<String, ColumnValue> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row.insert("score".to_string(), ColumnValue::Float64(95.5));
        row.insert("active".to_string(), ColumnValue::Bool(true));
        row.insert("nullable".to_string(), ColumnValue::Null);
        row
    }

    #[test]
    fn test_simple_comparison() {
        let row = make_row();

        let expr = parse_expr("score > 90").unwrap();
        assert!(eval_expr(&expr, &row));

        let expr = parse_expr("score < 90").unwrap();
        assert!(!eval_expr(&expr, &row));

        let expr = parse_expr("id == 1").unwrap();
        assert!(eval_expr(&expr, &row));

        let expr = parse_expr("name == 'Alice'").unwrap();
        assert!(eval_expr(&expr, &row));
    }

    #[test]
    fn test_and_or() {
        let row = make_row();

        let expr = parse_expr("score > 90 AND id == 1").unwrap();
        assert!(eval_expr(&expr, &row));

        let expr = parse_expr("score > 90 AND id == 2").unwrap();
        assert!(!eval_expr(&expr, &row));

        let expr = parse_expr("score < 90 OR id == 1").unwrap();
        assert!(eval_expr(&expr, &row));
    }

    #[test]
    fn test_not() {
        let row = make_row();

        let expr = parse_expr("NOT score < 90").unwrap();
        assert!(eval_expr(&expr, &row));

        let expr = parse_expr("NOT score > 90").unwrap();
        assert!(!eval_expr(&expr, &row));
    }

    #[test]
    fn test_is_null() {
        let row = make_row();

        let expr = parse_expr("nullable IS NULL").unwrap();
        assert!(eval_expr(&expr, &row));

        let expr = parse_expr("score IS NULL").unwrap();
        assert!(!eval_expr(&expr, &row));

        let expr = parse_expr("score IS NOT NULL").unwrap();
        assert!(eval_expr(&expr, &row));
    }

    #[test]
    fn test_parentheses() {
        let row = make_row();

        let expr = parse_expr("(score > 90) AND (id == 1)").unwrap();
        assert!(eval_expr(&expr, &row));

        let expr = parse_expr("(score < 90 OR id == 1) AND active == true").unwrap();
        assert!(eval_expr(&expr, &row));
    }
}
