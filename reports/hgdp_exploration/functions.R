guess_file_type <- function(x) {
  dplyr::case_when(
    grepl("\\.cram$", x) ~ "CRAM",
    grepl("\\.crai$", x) ~ "CRAMindex",
    grepl("\\.g\\.vcf\\.gz$$", x) ~ "GVCF",
    grepl("\\.g\\.vcf\\.gz.tbi$$", x) ~ "GVCFindex",
    grepl("\\.vcf\\.gz$$", x) ~ "VCF",
    grepl("\\.vcf\\.gz.tbi$$", x) ~ "VCFindex",
    grepl("\\.alignment_summary_metrics$", x) ~ "alignment_summary_metrics",
    grepl("\\.insert_size_metrics$", x) ~ "insert_size_metrics",
    grepl("\\.raw_wgs_metrics$", x) ~ "raw_wgs_metrics",
    grepl("\\.wgs_metrics$", x) ~ "wgs_metrics",
    grepl("\\.preBqsr\\.selfSM$", x) ~ "contamination_metrics",
    grepl("\\.md5$", x) ~ "MD5",
    TRUE ~ "OTHER")
}
