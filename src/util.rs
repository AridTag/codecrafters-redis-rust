use std::io;
use std::time::SystemTime;
use time::macros::format_description;

#[allow(unused)]
pub fn pretty_print_system_time(t: SystemTime) {
    pretty_print_system_time_into(&mut std::io::stdout().lock(), t)
}

pub fn pretty_print_system_time_into(output: &mut impl io::Write, t: SystemTime) {
    let utc = time::OffsetDateTime::UNIX_EPOCH
        + time::Duration::try_from(t.duration_since(std::time::UNIX_EPOCH).unwrap()).unwrap();
    utc
        .format_into(
            output,
            format_description!(
                "[day]-[month repr:short]-[year] [hour]:[minute]:[second]\n"
            ),
        )
        .unwrap();
}