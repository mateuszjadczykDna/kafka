#!/usr/bin/env groovy

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8'
    realJobPrefixes = ['ce-kafka']
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka'
    timeoutHours = 4
}


def job = {
    // Gradle test
    def gradlewParameters = "--no-daemon -Dorg.gradle.project.maxParallelForks=4 -Dorg.gradle.project.testLoggingEvents=started,passed,skipped,failed --stacktrace -Dorg.gradle.project.skipSigning=true"
    stage("Run Gradle tests") {
        sh "gradle"
        sh "./gradlew ${gradlewParameters} clean test"
    }

    // The Muckrake branch for running the downstream tests.
    def kafkaMuckrakeVersionMap = [
            "ce-trunk": "master"
    ];
    def kafkaRepo = "confluentinc/ce-kafka.git";
    def kafkaBranch = BRANCH_NAME;
    def muckrakeBranch = kafkaMuckrakeVersionMap[kafkaBranch];
    if (muckrakeBranch == null) {
        error "Unknown paired Muckrake branch for Kafka branch ${kafkaBranch}";
    }
    // Start the downstream job.
    stage("Trigger test-cp-downstream-builds") {
        echo "Schedule test-cp-downstream-builds with muckrake branch ${muckrakeBranch} and Apache Kafka branch ${kafkaBranch}."
        buildResult = build job: 'test-cp-downstream-builds', parameters: [
                [$class: 'StringParameterValue', name: 'BRANCH', value: muckrakeBranch],
                [$class: 'StringParameterValue', name: 'KAFKA_REPO', value: kafkaRepo],
                [$class: 'StringParameterValue', name: 'KAFKA_BRANCH', value: kafkaBranch],
                [$class: 'StringParameterValue', name: 'NODE_LABEL', value: "docker-oraclejdk8"]],
                propagate: true, wait: true
    }
}

def post = {
    stage('Upload results') {
        def summary = junit '**/build/test-results/**/TEST-*.xml'
        def total = summary.getTotalCount()
        def failed = summary.getFailCount()
        def skipped = summary.getSkipCount()
        summary = "Test results:\n\t"
        summary = summary + ("Passed: " + (total - failed - skipped))
        summary = summary + (", Failed: " + failed)
        summary = summary + (", Skipped: " + skipped)
        return summary;
    }
}

runJob config, job, post