//! Geospatial operations.
//!
//! Redis-compatible geo operations using geohash encoding.

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry, GeoData};
use std::sync::atomic::Ordering;

/// Geo operations trait
pub trait GeoOps {
    /// Add geo locations (GEOADD)
    fn geoadd(&mut self, key: String, locations: Vec<(f64, f64, String)>) -> usize;
    
    /// Get position of members (GEOPOS)
    fn geopos(&mut self, key: String, members: Vec<String>) -> Vec<Option<(f64, f64)>>;
    
    /// Get distance between two members (GEODIST)
    fn geodist(&mut self, key: String, member1: String, member2: String, unit: GeoUnit) -> Option<f64>;
    
    /// Get geohash of members (GEOHASH)
    fn geohash(&mut self, key: String, members: Vec<String>) -> Vec<Option<String>>;
    
    /// Search by radius from member (GEORADIUSBYMEMBER)
    fn georadiusbymember(&mut self, key: String, member: String, radius: f64, unit: GeoUnit, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult>;
    
    /// Search by radius from coordinates (GEORADIUS)
    fn georadius(&mut self, key: String, lon: f64, lat: f64, radius: f64, unit: GeoUnit, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult>;
    
    /// Search within box (GEOSEARCH)
    fn geosearch(&mut self, key: String, from: GeoFrom, by: GeoBy, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult>;
}

/// Distance unit
#[derive(Debug, Clone, Copy)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl GeoUnit {
    pub fn to_meters(&self, value: f64) -> f64 {
        match self {
            GeoUnit::Meters => value,
            GeoUnit::Kilometers => value * 1000.0,
            GeoUnit::Miles => value * 1609.344,
            GeoUnit::Feet => value * 0.3048,
        }
    }

    pub fn from_meters(&self, value: f64) -> f64 {
        match self {
            GeoUnit::Meters => value,
            GeoUnit::Kilometers => value / 1000.0,
            GeoUnit::Miles => value / 1609.344,
            GeoUnit::Feet => value / 0.3048,
        }
    }
}

/// Sort order for geo queries
#[derive(Debug, Clone, Copy)]
pub enum GeoSort {
    Asc,
    Desc,
}

/// Search from location
#[derive(Debug, Clone)]
pub enum GeoFrom {
    Member(String),
    LonLat(f64, f64),
}

/// Search by area
#[derive(Debug, Clone)]
pub enum GeoBy {
    Radius(f64, GeoUnit),
    Box(f64, f64, GeoUnit), // width, height
}

/// Geo search result
#[derive(Debug, Clone)]
pub struct GeoResult {
    pub member: String,
    pub distance: Option<f64>,
    pub coordinates: Option<(f64, f64)>,
    pub hash: Option<String>,
}

impl GeoOps for DB {
    fn geoadd(&mut self, key: String, locations: Vec<(f64, f64, String)>) -> usize {
        self.check_expiration(&key);

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::Geo(GeoData::new()),
            expires_at: None,
        });

        match &mut entry.value {
            DataType::Geo(geo) => {
                let mut added = 0;
                for (lon, lat, member) in locations {
                    if geo.add(member, lon, lat) {
                        added += 1;
                    }
                }
                if added > 0 {
                    self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                }
                added
            }
            _ => 0,
        }
    }

    fn geopos(&mut self, key: String, members: Vec<String>) -> Vec<Option<(f64, f64)>> {
        if !self.check_expiration(&key) {
            return vec![None; members.len()];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Geo(geo) = &entry.value {
                return members.iter().map(|m| {
                    geo.locations.get(m).map(|loc| (loc.longitude, loc.latitude))
                }).collect();
            }
        }
        vec![None; members.len()]
    }

    fn geodist(&mut self, key: String, member1: String, member2: String, unit: GeoUnit) -> Option<f64> {
        if !self.check_expiration(&key) {
            return None;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Geo(geo) = &entry.value {
                if let Some(distance_m) = geo.distance(&member1, &member2) {
                    return Some(unit.from_meters(distance_m));
                }
            }
        }
        None
    }

    fn geohash(&mut self, key: String, members: Vec<String>) -> Vec<Option<String>> {
        if !self.check_expiration(&key) {
            return vec![None; members.len()];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Geo(geo) = &entry.value {
                return members.iter().map(|m| {
                    geo.locations.get(m).map(|loc| {
                        geohash::encode(
                            geohash::Coord { x: loc.longitude, y: loc.latitude },
                            11
                        ).unwrap_or_default()
                    })
                }).collect();
            }
        }
        vec![None; members.len()]
    }

    fn georadiusbymember(&mut self, key: String, member: String, radius: f64, unit: GeoUnit, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Geo(geo) = &entry.value {
                if let Some(center) = geo.locations.get(&member) {
                    return self.search_radius(geo, center.longitude, center.latitude, radius, unit, count, sort);
                }
            }
        }
        vec![]
    }

    fn georadius(&mut self, key: String, lon: f64, lat: f64, radius: f64, unit: GeoUnit, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Geo(geo) = &entry.value {
                return self.search_radius(geo, lon, lat, radius, unit, count, sort);
            }
        }
        vec![]
    }

    fn geosearch(&mut self, key: String, from: GeoFrom, by: GeoBy, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Geo(geo) = &entry.value {
                let (lon, lat) = match &from {
                    GeoFrom::LonLat(lon, lat) => (*lon, *lat),
                    GeoFrom::Member(m) => {
                        if let Some(loc) = geo.locations.get(m) {
                            (loc.longitude, loc.latitude)
                        } else {
                            return vec![];
                        }
                    }
                };

                match by {
                    GeoBy::Radius(radius, unit) => {
                        return self.search_radius(geo, lon, lat, radius, unit, count, sort);
                    }
                    GeoBy::Box(width, height, unit) => {
                        return self.search_box(geo, lon, lat, width, height, unit, count, sort);
                    }
                }
            }
        }
        vec![]
    }
}

impl DB {
    fn search_radius(&self, geo: &GeoData, lon: f64, lat: f64, radius: f64, unit: GeoUnit, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult> {
        let radius_m = unit.to_meters(radius);
        
        let mut results: Vec<GeoResult> = geo.locations.iter()
            .filter_map(|(name, loc)| {
                let dist = haversine_distance(lat, lon, loc.latitude, loc.longitude);
                if dist <= radius_m {
                    Some(GeoResult {
                        member: name.clone(),
                        distance: Some(unit.from_meters(dist)),
                        coordinates: Some((loc.longitude, loc.latitude)),
                        hash: Some(geohash::encode(
                            geohash::Coord { x: loc.longitude, y: loc.latitude },
                            11
                        ).unwrap_or_default()),
                    })
                } else {
                    None
                }
            })
            .collect();

        // Sort
        if let Some(order) = sort {
            match order {
                GeoSort::Asc => results.sort_by(|a, b| {
                    a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
                }),
                GeoSort::Desc => results.sort_by(|a, b| {
                    b.distance.partial_cmp(&a.distance).unwrap_or(std::cmp::Ordering::Equal)
                }),
            }
        }

        if let Some(n) = count {
            results.truncate(n);
        }

        results
    }

    fn search_box(&self, geo: &GeoData, lon: f64, lat: f64, width: f64, height: f64, unit: GeoUnit, count: Option<usize>, sort: Option<GeoSort>) -> Vec<GeoResult> {
        let half_width_m = unit.to_meters(width) / 2.0;
        let half_height_m = unit.to_meters(height) / 2.0;
        
        // Approximate lat/lon deltas (not perfectly accurate but good enough)
        let lat_delta = half_height_m / 111320.0;
        let lon_delta = half_width_m / (111320.0 * lat.to_radians().cos());

        let mut results: Vec<GeoResult> = geo.locations.iter()
            .filter_map(|(name, loc)| {
                if (loc.latitude - lat).abs() <= lat_delta && (loc.longitude - lon).abs() <= lon_delta {
                    let dist = haversine_distance(lat, lon, loc.latitude, loc.longitude);
                    Some(GeoResult {
                        member: name.clone(),
                        distance: Some(unit.from_meters(dist)),
                        coordinates: Some((loc.longitude, loc.latitude)),
                        hash: Some(geohash::encode(
                            geohash::Coord { x: loc.longitude, y: loc.latitude },
                            11
                        ).unwrap_or_default()),
                    })
                } else {
                    None
                }
            })
            .collect();

        if let Some(order) = sort {
            match order {
                GeoSort::Asc => results.sort_by(|a, b| {
                    a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
                }),
                GeoSort::Desc => results.sort_by(|a, b| {
                    b.distance.partial_cmp(&a.distance).unwrap_or(std::cmp::Ordering::Equal)
                }),
            }
        }

        if let Some(n) = count {
            results.truncate(n);
        }

        results
    }
}

/// Haversine distance calculation
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS: f64 = 6371000.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geoadd_geopos() {
        let mut db = DB::new();
        
        let added = db.geoadd("sicily".to_string(), vec![
            (13.361389, 38.115556, "Palermo".to_string()),
            (15.087269, 37.502669, "Catania".to_string()),
        ]);
        assert_eq!(added, 2);

        let positions = db.geopos("sicily".to_string(), vec!["Palermo".to_string()]);
        assert!(positions[0].is_some());
        let (lon, lat) = positions[0].unwrap();
        assert!((lon - 13.361389).abs() < 0.0001);
        assert!((lat - 38.115556).abs() < 0.0001);
    }

    #[test]
    fn test_geodist() {
        let mut db = DB::new();
        
        db.geoadd("sicily".to_string(), vec![
            (13.361389, 38.115556, "Palermo".to_string()),
            (15.087269, 37.502669, "Catania".to_string()),
        ]);

        let dist = db.geodist("sicily".to_string(), 
            "Palermo".to_string(), 
            "Catania".to_string(), 
            GeoUnit::Kilometers);
        
        assert!(dist.is_some());
        let d = dist.unwrap();
        assert!(d > 160.0 && d < 170.0); // ~166km
    }
}
