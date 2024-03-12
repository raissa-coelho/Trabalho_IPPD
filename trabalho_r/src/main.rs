/*use rayon::prelude::*;
use mpi::traits::*;*/
use csv::Reader;
use std::error::Error;
use std::fs::File;

/* Processamento de arquivos CSV */
/*Estratégia:
MPI for inter-node communication (across different machines or processes)
Rayon for intra-node parallelism (within a single machine or process) */

/*Função para ler 1 arquivo CSV e retornar um vetor de inteiros*/
fn read_csv_records_as_integers(file_path: &str) -> Result<Vec<(Option<i32>, Option<i32>)>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);

    let mut records = Vec::new();

    for result in rdr.records() {
        let record = result?;
        let field1 = record.get(0).and_then(|s| s.parse::<i32>().ok());
        let field2 = record.get(1).and_then(|s| s.parse::<i32>().ok());
        records.push((field1, field2));
    }

    Ok(records)
}

fn main() {
    println!("Teste!");
    let arquivo1_path = "sample_integers.csv";
    let contents = read_csv_records_as_integers(arquivo1_path);

    match contents {
        Ok(data) => {
            println!("Valores :");
            for (first, second) in data {
                let first_str = first.map_or("N/A".to_string(), |num| num.to_string());
                let second_str = second.map_or("N/A".to_string(), |num| num.to_string());
                println!("({}, {})", first_str, second_str);
            }
        },
        Err(e) => {
            println!("An error occurred: {}", e);
        }
    }
}
