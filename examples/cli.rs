use clap::{Parser, Subcommand};
use serde_json::Value;
use smolkv_client::{Error, QueryBuilder, SmolKv, SortOrder};
use tokio_stream::StreamExt;

#[derive(Parser)]
#[command(name = "smolkv")]
#[command(about = "SmolKV CLI client", long_about = None)]
struct Cli {
    #[arg(long, env = "SMOLKV_ENDPOINT")]
    endpoint: String,

    #[arg(long, env = "SMOLKV_SECRET")]
    secret: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum CliSortOrder {
    Asc,
    Desc,
}

impl From<CliSortOrder> for SortOrder {
    fn from(order: CliSortOrder) -> Self {
        match order {
            CliSortOrder::Asc => SortOrder::Asc,
            CliSortOrder::Desc => SortOrder::Desc,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new collection
    Create {
        /// Collection name
        collection: String,
    },
    /// Delete a collection
    Drop {
        /// Collection name
        collection: String,
    },
    /// Put a value into a collection
    Put {
        /// Path in format <collection>/<key>
        path: String,
        /// JSON value to store
        value: String,
    },
    /// Get a value from a collection
    Get {
        /// Path in format <collection>/<key>
        path: String,
    },
    /// Delete a value
    Del {
        /// Path in format <collection>/<key>
        path: String,
    },
    List {
        /// Collection name
        collection: String,
        /// Start key for range
        #[arg(long)]
        from: Option<String>,
        /// End key for range
        #[arg(long)]
        to: Option<String>,
        /// Maximum number of results
        #[arg(long)]
        limit: Option<usize>,
        /// Sort order
        #[arg(long, value_enum, default_value_t = CliSortOrder::Asc)]
        order: CliSortOrder,
        /// Exclude keys from results
        #[arg(long, default_value_t = false)]
        no_keys: bool,
    },
    /// Watch for changes in a collection
    Watch {
        /// Collection name
        collection: String,
    },
    Query {
        /// JSONPath query expression
        query: String,
        /// Collection name
        #[arg(long)]
        collection: String,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<usize>,
        /// Include keys in the results
        #[arg(long, default_value_t = false)]
        no_keys: bool,
        /// Sort order (asc/desc)
        #[arg(long, value_enum, default_value_t = CliSortOrder::Asc)]
        order: CliSortOrder,
        /// Start key for range queries
        #[arg(long)]
        from: Option<String>,
        /// End key for range queries
        #[arg(long)]
        to: Option<String>,
    },
}

fn parse_path(path: &str) -> Result<(String, String), Error> {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match &parts[..] {
        [collection, key] => Ok((collection.to_string(), key.to_string())),
        _ => Err(Error::BadRequest(
            "Path must be in format <collection>/<key>".into(),
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    let kv = SmolKv::new(cli.endpoint, cli.secret);

    match cli.command {
        Commands::Create { collection } => {
            kv.create_collection(&collection).await?;
            println!("Collection '{collection}' created");
        }
        Commands::Drop { collection } => {
            kv.drop_collection(&collection).await?;
            println!("Collection '{collection}' dropped");
        }
        Commands::Put { path, value } => {
            let (collection, key) = parse_path(&path)?;
            let value: Value = serde_json::from_str(&value)?;
            kv.put(&collection, &key, &value).await?;
            println!("OK");
        }

        Commands::Get { path } => {
            let (collection, key) = parse_path(&path)?;
            let value: Value = kv.get(&collection, &key).await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }

        Commands::Del { path } => {
            let (collection, key) = parse_path(&path)?;
            kv.delete(&collection, &key).await?;
            println!("OK");
        }
        Commands::Watch { collection } => {
            let resp = kv.subscribe(&collection).await?;
            let mut stream = resp.bytes_stream();

            println!("Watching {collection}...");
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        let text = String::from_utf8_lossy(&bytes);
                        if let Some(data) = text.strip_prefix("data: ") {
                            if let Ok(event) = serde_json::from_str::<Value>(data) {
                                println!("{}", serde_json::to_string_pretty(&event)?);
                            }
                        }
                    }
                    Err(e) => eprintln!("Stream error: {e}"),
                }
            }
        }
        Commands::List {
            collection,
            from,
            to,
            limit,
            order,
            no_keys,
        } => {
            let builder = QueryBuilder::new()
                .from(from)
                .to(to)
                .limit(limit)
                .order(order)
                .keys(!no_keys);
            let items = kv.list_collection(&collection, builder).await?;
            println!("{}", serde_json::to_string_pretty(&items)?);
        }

        Commands::Query {
            collection,
            query,
            limit,
            no_keys,
            order,
            from,
            to,
        } => {
            let builder = QueryBuilder::new()
                .query(query)
                .keys(!no_keys)
                .from(from)
                .to(to)
                .limit(limit)
                .order(order);

            let results = kv.query_collection(&collection, builder).await?;
            println!("{}", serde_json::to_string_pretty(&results)?);
        }
    }

    Ok(())
}
