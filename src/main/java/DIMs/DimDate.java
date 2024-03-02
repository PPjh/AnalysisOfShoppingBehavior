package DIMs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DimDate {
    public static void main(String[] args) {
        // 设置时间范围
        int startYear = 2020;
        int endYear = 2023;
        // 设置日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        // 获取 Calendar 实例
        Calendar calendar = Calendar.getInstance();

        // 指定 CSV 文件路径
        String csvFilePath = "time_dimension_table.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath))) {
            // 写入 CSV 头部
            writer.write("Date,Year,Month,Day,Quarter,DayOfWeek\n");

            // 遍历时间范围
            for (int year = startYear; year <= endYear; year++) {
                // 设置年份
                calendar.set(Calendar.YEAR, year);
                // 月份从0开始，所以需要减1
                calendar.set(Calendar.MONTH, 0);
                // 设置为当月的第一天
                calendar.set(Calendar.DAY_OF_MONTH, 1);

                while (calendar.get(Calendar.YEAR) == year) {
                    // 构建 CSV 行数据
                    StringBuilder sb = new StringBuilder();
                    // 添加日期
                    sb.append(dateFormat.format(calendar.getTime())).append(",");
                    // 添加年份
                    sb.append(calendar.get(Calendar.YEAR)).append(",");
                    // 添加月份
                    sb.append((calendar.get(Calendar.MONTH) + 1)).append(",");
                    // 添加日
                    sb.append(calendar.get(Calendar.DAY_OF_MONTH)).append(",");
                    // 添加季度
                    int quarter = (calendar.get(Calendar.MONTH) / 3) + 1;
                    sb.append(quarter).append(",");
                    // 添加星期
                    sb.append(calendar.get(Calendar.DAY_OF_WEEK)).append("\n");

                    // 写入 CSV 行数据
                    writer.write(sb.toString());

                    // 设置为下一天
                    calendar.add(Calendar.DAY_OF_MONTH, 1);
                }
            }

            // 输出成功信息
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            // 输出错误信息
            System.err.println("Error writing CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
