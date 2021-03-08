version 1.0

# WORKFLOW DEFINITION

workflow PrepareGvcfsWf {
  input {
    Array[File] input_gvcfs
    Array[String] samples
    File input_regions = "gs://cpg-reference/hg38/v0/noalt.bed"
  }

  # Run the validation 
  scatter (input_gvcf in input_gvcfs) {
    String gvcf_filename = basename(input_gvcf)
    
    call Reblock {
        input:
          gvcf = input_gvcf,
          gvcf_index = input_gvcf + ".tbi",
          output_vcf_filename = gvcf_filename
    }
    
    call SubsetVcf {
        input:
          inputFile = Reblock.output_vcf,
          outputPath = gvcf_filename,
          regions = input_regions
    }
  }
  
  Array[File] output_gvcfs = SubsetVcf.output_vcf
  Array[File] output_gvcf_indices = SubsetVcf.output_vcf_index
  
  output {
    Array[File] gvcfs = select_all(output_gvcfs)
    Array[File] gvcf_indices = select_all(output_gvcf_indices)
  }
}

# TASK DEFINITIONS
  
task Reblock {
  input {
    File gvcf
    File gvcf_index
    String output_vcf_filename
  }

  command <<<

    gatk --java-options "-Xms3g -Xmx3g" \
    ReblockGVCF \
    -V ~{gvcf} \
    --drop-low-quals \
    -do-qual-approx \
    -O ~{output_vcf_filename} \
    --create-output-variant-index=true

  >>>
  runtime {
    memory: "3 GB"
    disks: "local-disk 10 HDD"
    preemptible: 3
    docker: "gcr.io/broad-dsde-methods/reblock_gvcf:correctedASCounts"
  }
  output {
    File output_vcf = "${output_vcf_filename}"
    File output_vcf_index = "${output_vcf_filename}.tbi"
  }
} 

task SubsetVcf {
    input {
        File inputFile
        String outputPath
        File regions
    }

    command {
        set -e
        mkdir -p "$(dirname ~{outputPath})"
        bcftools view \
        ~{inputFile} \
        -T ~{regions} \
        -o ~{outputPath} \
        -Oz

        bcftools index --tbi ~{outputPath}
    }

    output {
        File output_vcf = outputPath
        File output_vcf_index = outputPath + ".tbi"
    }

    runtime {
        memory: "3 GB"
        docker: "quay.io/biocontainers/bcftools:1.10.2--h4f4756c_2"
    }
}
