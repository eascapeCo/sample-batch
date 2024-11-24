package kr.excorp.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.policy.CompositeCompletionPolicy;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.batch.repeat.policy.TimeoutTerminationPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@RequiredArgsConstructor
public class CustomChunkPolicyJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job customPolicyJob() {
        //reader 1
        //reader 2
        //reader 3
        //processor 1
        //processor 2
        //processor 3
        //writer 2, 4, 6
        //reader 4
        //reader 5
        //processor 4
        //processor 5
        //writer 8, 10
        //reader 6
        //reader 7
        //reader 8
        //processor 6
        //processor 7
        //processor 8
        //writer 12, 14, 16
        //reader 9
        //reader 10
        //processor 9
        //processor 10
        //writer 18, 20
        return jobBuilderFactory.get("readerProcessorWriterJob")
                .start(customPolicyStep())
                .build();
    }

    @Bean
    @JobScope
    public Step customPolicyStep() {
        return stepBuilderFactory.get("readerProcessorWriterStep")
                .<Integer, String>chunk(customPolicy())
                .reader(new ItemReader<>() {
                    int i = 0;
                    @Override
                    public Integer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                        i++;
                        if (i <= 10) {
                            if (i == 5) {
                                Thread.sleep(200);
                            }
                            System.out.println("reader " + i);
                            return i;
                        }
                        return null;
                    }
                })
                .processor((ItemProcessor<Integer, String>) integer -> {
                    System.out.println("processor " + integer);
                    return String.valueOf(integer * 2);
                })
                .writer(list -> System.out.println("writer " + String.join(", ", list)))
                .build();
    }

    @Bean
    public CompletionPolicy customPolicy() {
        //3개 읽거나 100ms 넘어가면 chunk 작업 실행
        CompositeCompletionPolicy policy = new CompositeCompletionPolicy();
        policy.setPolicies(new CompletionPolicy[]{
                new SimpleCompletionPolicy(3),
                new TimeoutTerminationPolicy(100)   //100ms
        });

        return policy;
    }
}
