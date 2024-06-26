use fantoch::planet::Region;
use fantoch_exp::Protocol;

pub struct PlotFmt;

impl PlotFmt {
    pub fn region_name(region: Region) -> &'static str {
        match region.name().as_str() {
            "ap-southeast-1" => "Singapore",
            "ca-central-1" => "Canada",
            "eu-west-1" => "Ireland",
            "sa-east-1" => "S. Paulo", // São Paulo
            "us-west-1" => "N. California", // Northern California
            name => {
                panic!("PlotFmt::region_name: name {} not supported!", name);
            }
        }
    }

    pub fn protocol_name(protocol: Protocol) -> &'static str {
        match protocol {
            Protocol::AtlasLocked => "Atlas",
            Protocol::EPaxosLocked => "EPaxosLocked",
            Protocol::EPaxos=> "EPaxos",
            Protocol::CaesarLocked => "Caesar",
            Protocol::FPaxos => "FPaxos",
            Protocol::TempoAtomic => "Tempo",
            Protocol::TempoLocked => "Tempo-L",
            Protocol::Basic => "Inconsistent",
        }
    }

    pub fn label(protocol: Protocol, f: usize) -> String {
        match protocol {
            Protocol::EPaxosLocked | Protocol::CaesarLocked => {
                Self::protocol_name(protocol).to_string()
            }
            _ => format!("{} f = {}", Self::protocol_name(protocol), f),
        }
    }

    pub fn color(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "#27ae60",
            (Protocol::AtlasLocked, 2) => "#16a085",
            (Protocol::AtlasLocked, 3) => "#2980b9", // "#111111"
            (Protocol::EPaxosLocked, _) => "#444444",
            (Protocol::EPaxos, _) => "#444444",
            (Protocol::CaesarLocked, _) => "#bdc3c7",
            (Protocol::FPaxos, 1) => "#2980b9",
            (Protocol::FPaxos, 2) => "#34495e",
            (Protocol::TempoAtomic, 1) => "#f1c40f",
            (Protocol::TempoAtomic, 2) => "#e67e22",
            (Protocol::TempoAtomic, 3) => "#c23616", // "#333333"
            (Protocol::TempoLocked, 1) => "#2980b9",
            (Protocol::TempoLocked, 2) => "#c0392b",
            (Protocol::Basic, _) => "#576574",
            _ => panic!(
                "PlotFmt::color: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }.to_string()
    }

    pub fn background_color(protocol: Protocol) -> String {
        match protocol {
            Protocol::AtlasLocked => "#ecf0f1",
            Protocol::FPaxos => "#95a5a6",
            Protocol::TempoAtomic => "#353b48",
            _ => panic!(
                "PlotFmt::background_color: protocol = {:?} not supported!",
                protocol
            ),
        }
        .to_string()
    }

    // Possible values: {'/', '\', '|', '-', '+', 'x', 'o', 'O', '.', '*'}
    pub fn hatch(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::FPaxos, 1) => "/", // 1
            (Protocol::FPaxos, 2) => "\\",
            (Protocol::EPaxosLocked, _) => "//", // 2
            (Protocol::CaesarLocked, _) => "\\\\",
            (Protocol::AtlasLocked, 1) => "///", // 3
            (Protocol::AtlasLocked, 2) => "\\\\\\",
            (Protocol::TempoLocked, 1) => "////", // 4
            (Protocol::TempoLocked, 2) => "\\\\\\\\",
            (Protocol::TempoAtomic, 1) => "//////", //  6
            (Protocol::TempoAtomic, 2) => "\\\\\\\\\\\\",
            (Protocol::Basic, _) => "\\\\\\\\\\\\", // 6
            _ => panic!(
                "PlotFmt::hatch: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }.to_string()
    }

    // Possible values: https://matplotlib.org/3.1.1/api/markers_api.html#module-matplotlib.markers
    pub fn marker(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "o",
            (Protocol::AtlasLocked, 2) => "s",
            (Protocol::AtlasLocked, 3) => "P",
            (Protocol::EPaxosLocked, _) => "D",
            (Protocol::CaesarLocked, _) => "H",
            (Protocol::FPaxos, 1) => "+",
            (Protocol::FPaxos, 2) => "x",
            (Protocol::TempoAtomic, 1) => "v",
            (Protocol::TempoAtomic, 2) => "^",
            (Protocol::TempoAtomic, 3) => "p",
            (Protocol::TempoLocked, 1) => "o",
            (Protocol::TempoLocked, 2) => "s",
            (Protocol::Basic, _) => "P",
            _ => panic!(
                "PlotFmt::marker: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }.to_string()
    }

    // Possible values:  {'-', '--', '-.', ':', ''}
    pub fn linestyle(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::AtlasLocked, _) => "--",
            (Protocol::EPaxosLocked, _) => ":",
            (Protocol::EPaxos, _) => ":",
            (Protocol::CaesarLocked, _) => ":",
            (Protocol::FPaxos, _) => "-.",
            (Protocol::TempoAtomic, _) => "-",
            (Protocol::TempoLocked, _) => "-",
            (Protocol::Basic, _) => "-.",
        }
        .to_string()
    }

    pub fn linewidth(_f: usize) -> String {
        "1.6".to_string()
    }
}
