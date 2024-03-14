use csv::{Reader, WriterBuilder};
use mpi::traits::*;
use rayon::prelude::*;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::env;

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

    // Escrever registros 
    let mut wtr = WriterBuilder::new().from_writer(output_file);
    wtr.write_record(rdr.headers()?)?;

    // preciso desse código para não perder a 1a linha do segundo arquivo
    // tem que ser no rank 0 senão replica
    if rank == 0 {
        wtr.write_record(rdr2.headers()?)?;
    }

    // vetor em paralelo
    let all_records: Vec<_> = records1
        .into_par_iter()
        .chain(records2.into_par_iter())
        .collect();

    all_records
        .into_iter()
        .try_for_each(|record| -> Result<(), Box<dyn Error>> {
            wtr.write_record(&record)?;
            Ok(())
        })?;

    Ok(())
}

fn add_csv_columns_and_output(
    file_path1: &str,
    file_path2: &str,
    rank: i32,
) -> Result<(), Box<dyn Error>> {
    let output_file_path = PathBuf::from(format!("somado_{}.csv", rank));

    // Arquivo 1
    let mut rdr1 = Reader::from_path(file_path1)?;

    // Arquivo 2
    let mut rdr2 = Reader::from_path(file_path2)?;

    // Arquivo de output
    let output_file = File::create(&output_file_path)?;

    let records1: Vec<_> = rdr1.records().collect::<Result<Vec<_>, _>>()?;
    let records2: Vec<_> = rdr2.records().collect::<Result<Vec<_>, _>>()?;

    let results: Vec<_> = records1.into_par_iter().zip(records2.into_par_iter())
        .filter_map(|(record1, record2)| {
            let value1: i32 = record1.get(0).and_then(|v| v.parse().ok())?;
            let value2: i32 = record1.get(1).and_then(|v| v.parse().ok())?;
            let value3: i32 = record2.get(0).and_then(|v| v.parse().ok())?;
            let value4: i32 = record2.get(1).and_then(|v| v.parse().ok())?;

            let sum1 = value1 + value2;
            let sum2 = value3 + value4;

            Some(vec![sum1.to_string(), sum2.to_string()])
        })
        .collect();


    // Escrever registros sequêncialmente para evitar corrupção de dados
    let mut wtr = WriterBuilder::new().from_writer(output_file);

    for result in results {
        wtr.write_record(&result)?;
    }

    // Pra verificar que todo o buffer foi escrito antes de sair da fn
    wtr.flush()?;

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
    // args da linha de comando
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        println!("Usar: {} <file1.csv> <file2.csv> <file3.csv> <file4.csv>", args[0]);
        return;
    }

    let ar1 = &args[1];
    let ar2 = &args[2];
    let ar3 = &args[3];
    let ar4 = &args[4];

    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();

    if rank == 0 {
        println!("Arquivos CSV");
    }
    // número de colunas de cada um
    let a = checa_size(ar1).unwrap();
    let b = checa_size(ar2).unwrap();
    let c = checa_size(ar3).unwrap();
    let d = checa_size(ar4).unwrap();

    // checa se possuem o mesmo número de colunas
    if a == b && b == c && c == d {
        let start = Instant::now();

        world.barrier();

        if rank == 0 {
            if let Err(err) = add_csv_columns_and_output(ar1, ar2, rank)
            {
                eprintln!("Error: {}", err);
            }
            if let Err(err) =
                add_csv_columns_and_output(ar3, ar4, rank + 1)
            {
                eprintln!("Error: {}", err);
            }

            println!("Juntando...");

            if let Err(err) = merge_csv("somado_0.csv", "somado_1.csv", rank) {
                eprintln!("Error: {}", err);
            }
            if rank == 0 {
                let fim = start.elapsed();
                let elapsed_secs = fim.as_secs() as f64 + f64::from(fim.subsec_millis()) / 1000.0;
                println!("Concluído em {:.3}s.", elapsed_secs);
            }
        }
    } else {
        println!("Arquivos CSV diferentes");
    }
}
