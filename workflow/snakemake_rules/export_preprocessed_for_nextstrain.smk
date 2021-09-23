#
# Deployment and error handlers, including Slack messaging integrations.
#

from os import environ

SLACK_TOKEN   = environ["SLACK_TOKEN"]   = config["slack_token"]   or ""
SLACK_CHANNEL = environ["SLACK_CHANNEL"] = config["slack_channel"] or ""

try:
    deploy_origin = (
        f"from AWS Batch job `{environ['AWS_BATCH_JOB_ID']}`"
        if environ.get("AWS_BATCH_JOB_ID") else
        f"by the hands of {getuser()}@{getfqdn()}"
    )
except:
    # getuser() and getfqdn() may not always succeed, and this catch-all except
    # means that the Snakefile won't crash.
    deploy_origin = "by an unknown identity"


rule mutation_summary:
    message: "Summarizing {input.alignment}"
    input:
        alignment = rules.align.output.alignment,
        insertions = rules.align.output.insertions,
        translations = rules.align.output.translations,
        reference = config["files"]["alignment_reference"],
        genemap = config["files"]["annotation"]
    output:
        mutation_summary = "results/mutation_summary_{origin}.tsv.xz"
    log:
        "logs/mutation_summary_{origin}.txt"
    benchmark:
        "benchmarks/mutation_summary_{origin}.txt"
    params:
        outdir = "results/translations",
        basename = "seqs_{origin}",
        genes=config["genes"],
    conda: config["conda_environment"]
    shell:
        """
        python3 scripts/mutation_summary.py \
            --alignment {input.alignment} \
            --insertions {input.insertions} \
            --directory {params.outdir} \
            --basename {params.basename} \
            --reference {input.reference} \
            --genes {params.genes:q} \
            --genemap {input.genemap} \
            --output {output.mutation_summary} 2>&1 | tee {log}
        """


rule upload:
    message: "Uploading preprocesing (intermediate) files for specified origins to {params.s3_bucket}"
    input:
        unpack(_get_preprocessed_upload_inputs)
    params:
        s3_bucket = config["S3_DST_BUCKET"],
    log:
        "logs/upload.txt"
    benchmark:
        "benchmarks/upload.txt"
    run:
        for remote, local in input.items():
            shell("./scripts/upload-to-s3 {local:q} s3://{params.s3_bucket:q}/{remote:q} | tee -a {log:q}")

onstart:
    slack_message = f"Preprocessing build {deploy_origin} started."

    if SLACK_TOKEN and SLACK_CHANNEL:
        shell(f"""
            curl https://slack.com/api/chat.postMessage \
                --header "Authorization: Bearer $SLACK_TOKEN" \
                --form-string channel="$SLACK_CHANNEL" \
                --form-string text={{slack_message:q}} \
                --fail --silent --show-error \
                --include
        """)

onerror:
    slack_message = f"Preprocessing build {deploy_origin} failed."

    if SLACK_TOKEN and SLACK_CHANNEL:
        shell(f"""
            curl https://slack.com/api/files.upload \
                --header "Authorization: Bearer $SLACK_TOKEN" \
                --form-string channels="$SLACK_CHANNEL" \
                --form-string initial_comment={{slack_message:q}} \
                --form file=@{{log:q}} \
                --form filetype=text \
                --fail --silent --show-error \
                --include
        """)
