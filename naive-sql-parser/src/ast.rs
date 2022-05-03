// `TB` for `table`, `DB` for `Database`

pub use crate::defs::*;

#[derive(Debug)]
pub enum SqlStmt {
    CreateDB(Box<CreateDB>),
    CreateTB(Box<CreateTB>),
    CreateIdx(Box<CreateIdx>),
    DropDB(Box<DropDB>),
    DropTB(Box<DropTB>),
    DropIdx(Box<DropIdx>),
    Select(Box<Select>),
    Insert(Box<Insert>),
    Update(Box<Update>),
    Delete(Box<Delete>),
    UseDB(Box<UseDB>),
    Show(Box<Show>),
    Desc(Box<Desc>),
    Alter(Box<Alter>),
}

#[derive(Debug)]
pub enum Alter {
    CreateIdx(CreateIdx),
    DropIdx(DropIdx),
    AddPrimary(AddPrimary),
    AddForeign(AddForeign),
    DropForeign(DropForeign),
}

#[derive(Debug)]
pub struct CreateDB(pub String);

#[derive(Debug)]
pub struct CreateTB {
    pub name: String,
    pub fields: Vec<CreateTBField>,
}

#[derive(Debug)]
pub enum CreateTBField {
    Constraint(NamedTBConstraint),
    Column(Column),
}

#[derive(Debug)]
pub struct CreateIdx {
    pub table_name: String,
    pub fields: Vec<String>,
}

#[derive(Debug)]
pub struct DropDB(pub String);

#[derive(Debug)]
pub struct DropTB(pub String);

#[derive(Debug)]
pub struct DropIdx {
    pub cols: Vec<String>,
    pub table_name: String,
}

#[derive(Debug)]
pub struct AddPrimary {
    pub table_name: String,
    pub cols: Vec<String>,
}

#[derive(Debug)]
pub struct AddForeign {
    pub table_name: String,
    pub cols: Vec<String>,
    pub ftable_name: String,
    pub fcols: Vec<String>,
}

#[derive(Debug)]
pub struct DropForeign {
    pub table_name: String,
    pub cols: Vec<String>,
    pub ftable_name: String,
    pub fcols: Vec<String>,
}

#[derive(Debug)]
pub struct Select {
    pub selectors: Selectors,
    pub from: Vec<String>,
    pub condition: Option<CondExpr>,
    pub group_by: Option<ColumnRef>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}

#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug)]
pub struct Update {
    pub table_name: String,
    pub column: ColumnRef,
    pub value: Expr,
    pub condition: CondExpr,
}

#[derive(Debug)]
pub struct Delete {
    pub table_name: String,
    pub condition: CondExpr,
}

#[derive(Debug)]
pub struct UseDB(pub String);

#[derive(Debug)]
pub enum Show {
    Databases,
    Tables,
    Indices,
}

#[derive(Debug)]
pub struct Desc(pub String);
