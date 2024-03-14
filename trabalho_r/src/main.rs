use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use csv::{Reader, WriterBuilder};
use mpi::traits::*;
use rayon::prelude::*;

/* Processamento de arquivos CSV */
/* Estratégia:
MPI for inter-node communication (across different machines or processes)
Rayon for intra-node parallelism (within a single machine or process) */

fn merge_csv<P: AsRef<Path>>(file_path: P, file_path2: P, rank: i32) -> Result<(), Box<dyn Error>> {
    let output_path = PathBuf::from(format!("merged_{}.csv", rank));

    // Arquivo 1
    let file = File::open(&file_path)?;
    let mut rdr = Reader::from_reader(file);

    // Arquivo 2
    let file2 = File::open(&file_path2)?;
    let mut rdr2 = Reader::from_reader(file2);

    // Arquivo de output
    let output_file = File::create(&output_path)?;

    // Coletar registros do arquivo 1
    let records1: Vec<_> = rdr.records().collect::<Result<Vec<_>, _>>()?;

    // Coletar registros do arquivo 2
    let records2: Vec<_> = rdr2.records().collect::<Result<Vec<_>, _>>()?;

    // Escrever registros em paralelo
    let mut wtr = WriterBuilder::new().from_writer(output_file);
    if rank == 0 {
        wtr.write_record(rdr.headers()?)?;
    }

    // vetor em paralelo
    let all_records: Vec<_> = records1.into_par_iter().chain(records2.into_par_iter()).collect();

    all_records.into_iter().try_for_each(|record| -> Result<(), Box<dyn Error>> {
        wtr.write_record(&record)?;
        Ok(())
    })?;

    Ok(())
}

// checa número de colunas
fn checa_size<P: AsRef<Path>>(file1: P) -> Result<usize, Box<dyn Error>> {
    // Arquivo
    let file = File::open(file1)?;
    let mut rdr = Reader::from_reader(file);

    let headers = rdr.headers()?.clone();

    Ok(headers.len())
}

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();

    println!("Arquivos CSV");

    // número de colunas de cada um
    let a = checa_size("car_prices.csv").unwrap();
    let b = checa_size("car_prices2.csv").unwrap();

    // checa se possuem o mesmo número de colunas
    if a == b {
        let start = Instant::now();

        println!("Juntando...");

        if let Err(err) = merge_csv("car_prices.csv", "car_prices2.csv", rank) {
            eprintln!("Error: {}", err);
        }

        world.barrier();

        if rank == 0 {
            let fim = start.elapsed();
            let elapsed_secs = fim.as_secs() as f64 + f64::from(fim.subsec_millis()) / 1000.0;
            println!("Concluído em {:.3}s.", elapsed_secs);
        }

    } else {
        if rank == 0 {
            println!("Arquivos CSV diferentes");
        }
    }
}
