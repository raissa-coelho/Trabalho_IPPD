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

    let records = Vec::new();
    
    type Record = (f64, f64);
    for result in rdr.deserialize(){
        let records: Record = result?;
        println!("{:?}", records);
    }

    Ok(records)
}



fn main() {
    
    println!("Merge de Arquivos CSV");
    let arquivo1_path = "sample_integers.csv";
    let arquivo2_path: &str = "sample_integers2.csv";

    if let Err(err) = read_csv_records_as_integers(arquivo1_path) {
        eprintln!("Erro: {}", err);
    }

    if let Err(err) = read_csv_records_as_integers(arquivo2_path) {
        eprintln!("Erro: {}", err);
    }
}
