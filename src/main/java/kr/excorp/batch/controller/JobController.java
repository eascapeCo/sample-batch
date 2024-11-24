package kr.excorp.batch.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Date;

@Controller
@RequiredArgsConstructor
public class JobController {

    private final JobLauncher jobLauncher;
    private final Job simpleTaskletJob;
    private final Job parameterJob;
    private final Job readerProcessorWriterJob;
    private final Job customPolicyJob;


    @GetMapping("/tasklet")
    @ResponseBody
    public String runJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("startDate", new Date())
                .toJobParameters();

        jobLauncher.run(simpleTaskletJob, jobParameters);
        return "its works!!";
    }

    @GetMapping("/tasklet-autoincrement")
    @ResponseBody
    public String runAutoIncrementJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .getNextJobParameters(simpleTaskletJob)
                .toJobParameters();

        jobLauncher.run(simpleTaskletJob, jobParameters);
        return "its works!!";
    }

    @GetMapping("/parameter")
    @ResponseBody
    public String runParameterJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("startDate", new Date())
                .addString("name", "testing")
                .toJobParameters();

        jobLauncher.run(parameterJob, jobParameters);
        return "its works!!";
    }

    @GetMapping("/readProcessorWriter")
    @ResponseBody
    public String runReaderProcessorWriterJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("startDate", new Date())
                .toJobParameters();

        jobLauncher.run(readerProcessorWriterJob, jobParameters);
        return "its works!!";
    }

    @GetMapping("/customPolicyJob")
    @ResponseBody
    public String customPolicyJobJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("startDate", new Date())
                .toJobParameters();

        jobLauncher.run(customPolicyJob, jobParameters);
        return "its works!!";
    }
}
