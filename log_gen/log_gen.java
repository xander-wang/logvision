/*
    log_gen｜模拟日志生成器 v2.0 by Xander on 2020.2.24
    
    这一简单的Java实现可根据传入参数（每次写入行数、写入间隔时间）将样本日志中的特定
    行块追加至目标日志中，以模拟日志生成，供后续实时处理。

    仅供测试，用了BufferedWriter但实际并未缓存，立即写入。

    #############################################################

    log_gen v2.0 by Xander on June 24th, 2020

    This simple Java implementation is capable of appending specific block
    of lines into target log file based on given number of lines and time 
    interval to better simulate log appending process, which provides a 
    streaming file source for real-time log processing.

    For Testing only, BufferedWriter is used but no buffer is actually
    exist, the data's written to the file immediately.
*/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

class LogAppender {
    public static void main(String[] args) {
        Charset charset = Charset.forName("UTF-8");

        int linesEachTime = Integer.parseInt(args[2]);
        int interval = Integer.parseInt(args[3]);

        Path srcLog = FileSystems.getDefault().getPath(args[0]);
        Path tarLog = FileSystems.getDefault().getPath(args[1]);

        if(args.length < 4) {
            System.out.println("Usage: [LOG_SRC_PATH] [LOG_TARGET_PATH] [NUMBER_OF_LINES_EACH_CYCLE] [TIME_INTERVAL]");
        }
        
        try {
            BufferedReader readerForLineCount = Files.newBufferedReader(srcLog, charset);
            BufferedReader reader = Files.newBufferedReader(srcLog, charset);
            BufferedWriter writer = Files.newBufferedWriter(tarLog, charset);

            LineNumberReader lineNumberReader = new LineNumberReader(readerForLineCount);
            lineNumberReader.skip(Long.MAX_VALUE);
            int totalLine = lineNumberReader.getLineNumber();

            // load and append
            int i = 0;
            int current = 0;
            StringBuilder buffer = new StringBuilder();     // better performance using StringBuilder? 
            String line = null;

            while(current < totalLine) {
                // load to buffer
                for (i = 0; i < linesEachTime; i++){
                    line = reader.readLine();
                    buffer.append(line + "\n");
                }
                current += linesEachTime;
                System.out.printf("\n------------------------\n[LogVision] %d lines buffered, ready to append;\n########################\n", linesEachTime);
                System.out.print(buffer.toString());

                // append as a whole
                writer.write(buffer.toString(), 0, buffer.toString().length());
                writer.flush();
                System.out.printf("########################\n[LogVision] %d lines appended, going to sleep for %d seconds;\n------------------------\n", linesEachTime, interval);
                
                buffer.delete(0, buffer.length());
                Thread.sleep(interval * 1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}