use reqwest::{Client, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
mod errors;
pub use errors::Error;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchOperation<T> {
    pub key: String,
    pub value: T,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CollectionEvent {
    pub operation: String,
    pub key: String,
    pub value: Value,
    #[serde(default)]
    pub server_time: Option<u64>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct QueryBuilder {
    from: Option<String>,
    to: Option<String>,
    limit: Option<usize>,
    order: Option<SortOrder>,
    #[serde(default)]
    keys: bool,
    query: Option<String>,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    pub fn keys(mut self, include_keys: bool) -> Self {
        self.keys = include_keys;
        self
    }

    pub fn order(mut self, order: impl Into<SortOrder>) -> Self {
        self.order = Some(order.into());
        self
    }

    pub fn from(mut self, from: Option<impl Into<String>>) -> Self {
        self.from = from.map(Into::into);
        self
    }

    pub fn to(mut self, to: Option<impl Into<String>>) -> Self {
        self.to = to.map(Into::into);
        self
    }

    pub fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Clone)]
pub struct SmolKv {
    endpoint: String,
    client: Client,
}

impl SmolKv {
    pub fn new(endpoint: impl Into<String>, secret: Option<impl Into<String>>) -> Self {
        let client = match secret {
            Some(secret_key) => {
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("X-SECRET-KEY", secret_key.into().parse().unwrap());
                Client::builder().default_headers(headers).build().unwrap()
            }
            None => Client::new(),
        };

        Self {
            endpoint: endpoint.into(),
            client,
        }
    }

    fn url(&self, path: impl AsRef<str>) -> String {
        let path = path.as_ref().trim_start_matches('/');
        format!("{}/api/{}", self.endpoint, path)
    }

    async fn handle_response<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T> {
        match resp.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(resp.json().await?),
            StatusCode::NOT_FOUND => Err(Error::NotFound(resp.url().path().to_string())),
            StatusCode::CONFLICT => Err(Error::AlreadyExists(resp.url().path().to_string())),
            StatusCode::BAD_REQUEST => Err(Error::BadRequest(resp.text().await?)),
            s => Err(Error::Server(format!("unexpected status: {}", s))),
        }
    }

    // collection operations
    pub async fn collection_exists(&self, name: &str) -> Result<bool> {
        Ok(self
            .client
            .head(self.url(name))
            .send()
            .await?
            .status()
            .is_success())
    }

    pub async fn create_collection(&self, name: &str) -> Result<Value> {
        let resp = self.client.put(self.url(name)).send().await?;

        Self::handle_response(resp).await
    }

    pub async fn drop_collection(&self, name: &str) -> Result<Value> {
        let resp = self
            .client
            .delete(self.url(format!("/{}", name)))
            .send()
            .await?;

        Self::handle_response(resp).await
    }

    pub async fn list_collection(&self, name: &str, query: QueryBuilder) -> Result<Vec<Value>> {
        let resp = self.client.get(self.url(name)).query(&query).send().await?;
        Self::handle_response(resp).await
    }

    pub async fn query_collection(&self, name: &str, query: QueryBuilder) -> Result<Vec<Value>> {
        let resp = self.client.post(self.url(name)).json(&query).send().await?;
        Self::handle_response(resp).await
    }
    // key operations
    pub async fn get<T: DeserializeOwned>(&self, collection: &str, key: &str) -> Result<T> {
        let resp = self
            .client
            .get(self.url(format!("{collection}/{key}")))
            .send()
            .await?;

        Self::handle_response(resp).await
    }

    pub async fn put<T: Serialize>(&self, collection: &str, key: &str, value: &T) -> Result<Value> {
        let resp = self
            .client
            .put(self.url(format!("{collection}/{key}")))
            .json(value)
            .send()
            .await?;

        Self::handle_response(resp).await
    }
    pub async fn import_values(
        &self,
        collection: &str,
        key: Option<String>,
        values: Vec<u8>,
    ) -> Result<Value> {
        let part = reqwest::multipart::Part::bytes(values).file_name("backup.sst");
        let form = reqwest::multipart::Form::new().part("file", part);

        let resp = self
            .client
            .post(self.url(format!("{collection}/_import")))
            .multipart(form)
            .query(&[("key", key)])
            .send()
            .await?;

        Self::handle_response(resp).await
    }

    pub async fn delete(&self, collection: &str, key: &str) -> Result<bool> {
        Ok(self
            .client
            .delete(self.url(format!("{collection}/{key}")))
            .send()
            .await?
            .status()
            .is_success())
    }

    pub async fn exists(&self, collection: &str, key: &str) -> Result<bool> {
        Ok(self
            .client
            .head(self.url(format!("{collection}/{key}")))
            .send()
            .await?
            .status()
            .is_success())
    }

    pub async fn batch_put<T: Serialize>(
        &self,
        collection: &str,
        items: &[BatchOperation<T>],
    ) -> Result<()> {
        let resp = self
            .client
            .put(self.url(format!("{collection}/_batch")))
            .json(&items)
            .send()
            .await?;

        Self::handle_response::<Value>(resp).await.map(|_| ())
    }

    pub async fn subscribe(&self, collection: &str) -> Result<reqwest::Response> {
        let resp = self
            .client
            .get(self.url(format!("{collection}/_subscribe")))
            .send()
            .await?;

        match resp.status() {
            StatusCode::OK => Ok(resp),
            _ => Err(Error::NotFound(collection.to_string())),
        }
    }
    pub async fn start_backup(&self, collection: &str) -> Result<Value> {
        let resp = self
            .client
            .post(self.url(format!("{collection}/_backup")))
            .send()
            .await?;

        Self::handle_response(resp).await
    }
    pub async fn backup_status(&self, collection: &str, id: &str) -> Result<Value> {
        let resp = self
            .client
            .get(self.url(format!("{collection}/_backup/status?id={id}")))
            .send()
            .await?;

        Self::handle_response(resp).await
    }
    pub async fn download_backup(&self, collection: &str, backup_id: &str) -> Result<bytes::Bytes> {
        let resp = self
            .client
            .get(format!(
                "{}/backups/{collection}-{backup_id}.sst",
                self.endpoint
            ))
            .send()
            .await?;

        match resp.status() {
            StatusCode::OK => Ok(resp.bytes().await?),
            StatusCode::NOT_FOUND => Err(Error::NotFound(backup_id.to_string())),
            s => Err(Error::Server(format!("unexpected status: {}", s))),
        }
    }
    pub async fn upload_backup(&self, collection: &str, backup_data: Vec<u8>) -> Result<Value> {
        let part = reqwest::multipart::Part::bytes(backup_data)
            .file_name(format!("{collection}-backup.sst"));

        let form = reqwest::multipart::Form::new().part("file", part);

        let resp = self
            .client
            .post(self.url(format!("{collection}/_backup/upload")))
            .multipart(form)
            .send()
            .await?;

        Self::handle_response(resp).await
    }
    pub async fn start_restore(&self, collection: &str, id: &str) -> Result<Value> {
        let resp = self
            .client
            .post(self.url(format!("{collection}/_restore?backup_id={id}")))
            .send()
            .await?;

        Self::handle_response(resp).await
    }

    pub async fn restore_status(&self, collection: &str, id: &str) -> Result<Value> {
        let resp = self
            .client
            .get(self.url(format!("{collection}/_restore/status?id={id}")))
            .send()
            .await?;

        Self::handle_response(resp).await
    }
}
