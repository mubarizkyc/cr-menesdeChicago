use futures::StreamExt;
use log::info;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use std::time::Duration;
//define chema for crimes
/*
ID","Case Number","Date","Block","IUCR","Primary Type","Description","Location Description","Arrest","Domestic","Beat","District","Ward","Community Area","FBI Code","X Coordinate","Y Coordinate","Year","Updated On","Latitude","Longitude","Location"
"14169019","JK220052","04/13/2026 12:00:00 AM","041XX N MENARD AVE","0486","BATTERY","DOMESTIC BATTERY SIMPLE","RESIDENCE","false","true","1624","016","38","15","08B","1136939","1926980","2026","04/20/2026 03:42:51 PM","41.955790482","-87.77197935","(41.955790482, -87.77197935)"
*/
#[derive(serde::Deserialize, serde::Serialize)]
struct Crime {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "Case Number")]
    case_number: String,
    #[serde(rename = "Date")]
    date: String,
    #[serde(rename = "Block")]
    block: String,
    #[serde(rename = "IUCR")]
    iucr: String,
    #[serde(rename = "Primary Type")]
    primary_type: String,
    #[serde(rename = "Description")]
    description: String,
    #[serde(rename = "Location Description")]
    location_description: String,
    #[serde(rename = "Arrest")]
    arrest: String,
    #[serde(rename = "Domestic")]
    domestic: String,
    #[serde(rename = "Beat")]
    beat: String,
    #[serde(rename = "District")]
    district: String,
    #[serde(rename = "Ward")]
    ward: String,
    #[serde(rename = "Community Area")]
    community_area: String,
    #[serde(rename = "FBI Code")]
    fbi_code: String,
    #[serde(rename = "X Coordinate")]
    x_coordinate: String,
    #[serde(rename = "Y Coordinate")]
    y_coordinate: String,
    #[serde(rename = "Year")]
    year: String,
    #[serde(rename = "Updated On")]
    updated_on: String,
    #[serde(rename = "Latitude")]
    latitude: String,
    #[serde(rename = "Longitude")]
    longitude: String,
    #[serde(rename = "Location")]
    location: String,
}
const ROWS_PER_SEC: u16 = 1;
#[tokio::main]
async fn main() {
    let topic_name = "crime_events";
    let broker = "localhost:9092";
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .set("linger.ms", "5") // wait 5ms to fill a batch
        .set("batch.size", "65536") // or until 64kb is reached — whichever first
        .create()
        .expect("err creating producer");
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path("data/crimes.csv")
        .expect("incorrect path");

    futures::stream::iter(csv_reader.deserialize::<Crime>())
        .map(|result| {
            let producer = producer.clone();
            async move {
                let record = result.expect("failed to des");
                let key = record.district.clone();
                let payload = serde_json::to_string(&record).unwrap();
                //  println!("key: {}", key);
                //println!("payload: {}", payload);
                producer
                    .send(
                        FutureRecord::to(topic_name).key(&key).payload(&payload),
                        Duration::from_secs(0),
                    )
                    .await
            }
        })
        .buffer_unordered(1000)
        .for_each(|result| async move {
            match result {
                Ok(delivery) => {
                    println!(
                        "parititon={} offset={} ",
                        delivery.partition, delivery.offset
                    )
                }
                Err((e, _)) => {
                    eprint!("kafka error: {} ", e)
                }
            }
        })
        .await;
}
