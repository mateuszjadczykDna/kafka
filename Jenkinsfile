#!/usr/bin/env groovy

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka'
    timeoutHours = 4
}


def job = {
    // Gradle test
    def gradlewParameters = "--no-daemon -Dorg.gradle.project.maxParallelForks=4 -Dorg.gradle.project.testLoggingEvents=started,passed,skipped,failed --stacktrace -Dorg.gradle.project.skipSigning=true"
    def kafkaRepo
    // For PR build, Jenkins sets the CHANGE_BRANCH to the branch name of the source repo, THE BRANCH_NAME to PR-changeid
    // For non-PR build, Jenkins sets the BRANCH_NAME to the branch name.
    def kafkaBranch = env.CHANGE_BRANCH ?: env.BRANCH_NAME;
    stage("Run Gradle tests") {
        kafkaRepo = sh(script: 'git config --get remote.origin.url', returnStdout: true);
        sh "gradle"
        sh "./gradlew ${gradlewParameters} clean test"
    }

    // The Muckrake branch for running the downstream tests.
    def kafkaMuckrakeVersionMap = [
            "ce-trunk": "master"
    ];
    def muckrakeBranch = kafkaMuckrakeVersionMap[kafkaBranch] ?: "master";
    // Start the downstream job.
    stage("Trigger test-cp-downstream-builds") {
        echo "Schedule test-cp-downstream-builds with muckrake branch ${muckrakeBranch} and Apache Kafka branch ${kafkaRepo}:${kafkaBranch}."
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