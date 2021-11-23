version 1.0 

task hello {
    input {
        String addressee  
    }
    command {
        echo "Hello ${addressee}! Welcome to Cromwell . . . on Google Cloud!"  
    }
    output {
        String message = read_string(stdout())
    }
    runtime {
        docker: "ubuntu:latest"
    }
}

workflow wf_hello {
    call hello

}
