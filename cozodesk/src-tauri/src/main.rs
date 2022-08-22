#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use std::sync::Mutex;

struct CozoAppState {
    db: Mutex<Option<cozo::Db>>,
}

#[tauri::command]
fn is_opened(state: tauri::State<CozoAppState>) -> bool {
    state.db.lock().unwrap().is_some()
}

#[tauri::command(async)]
fn open_db(path: String, state: tauri::State<CozoAppState>) -> Result<(), String> {
    let mut cur_db = state.db.lock().unwrap();
    let builder = cozo::DbBuilder::default()
        .path(&path)
        .create_if_missing(true);
    match cozo::Db::build(builder) {
        Err(e) => Err(format!("{:?}", e)),
        Ok(db) => {
            cur_db.replace(db);
            Ok(())
        }
    }
}

#[tauri::command(async)]
fn close_db(state: tauri::State<CozoAppState>) -> Result<(), String> {
    state.db.lock().unwrap().take();
    Ok(())
}

#[tauri::command(async)]
fn run_query(
    state: tauri::State<CozoAppState>,
    query: String,
) -> Result<serde_json::Value, String> {
    let opt_db = state.db.lock().unwrap();
    match &*opt_db {
        None => Err("no db opened".to_string()),
        Some(db) => match db.run_script(&query) {
            Err(e) => Err(format!("{:?}", e)),
            Ok(value) => Ok(value),
        },
    }
}

fn main() {
    tauri::Builder::default()
        .manage(CozoAppState {
            db: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![open_db, close_db, run_query, is_opened])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
