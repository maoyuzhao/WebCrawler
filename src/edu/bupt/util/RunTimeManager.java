package edu.bupt.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class RunTimeManager {

	/**
	 * cmd /c dir 是执行完dir命令后关闭命令窗口。
	 * cmd /k dir 是执行完dir命令后不关闭命令窗口。
	 * cmd /c start dir 会打开一个新窗口后执行dir指令，原窗口会关闭。
	 * cmd /k start dir 会打开一个新窗口后执行dir指令，原窗口不会关闭。
	 * @param 所要执行的命令行，注意在windows下执行注意加上上述命令前缀。
	 */
	public static void run(String[] args) {
		Runtime run = Runtime.getRuntime();// 返回与当前 Java 应用程序相关的运行时对象
		try {
			Process p = run.exec(args);// 启动另一个进程来执行命令
			BufferedInputStream in = new BufferedInputStream(p.getInputStream());
			BufferedReader inBr = new BufferedReader(new InputStreamReader(in));
			String lineStr;
			while ((lineStr = inBr.readLine()) != null)
				// 获得命令执行后在控制台的输出信息
				System.out.println(lineStr);// 打印输出信息
			// 检查命令是否执行失败。
			if (p.waitFor() != 0) {
				if (p.exitValue() == 1)// p.exitValue()==0表示正常结束，1：非正常结束
					System.err.println("命令执行失败!");
			}
			inBr.close();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * cmd /c dir 是执行完dir命令后关闭命令窗口。
	 * cmd /k dir 是执行完dir命令后不关闭命令窗口。
	 * cmd /c start dir 会打开一个新窗口后执行dir指令，原窗口会关闭。
	 * cmd /k start dir 会打开一个新窗口后执行dir指令，原窗口不会关闭。
	 * @param 所要执行的命令行，注意在windows下执行注意加上上述命令前缀。
	 */
	public static void run(String cmd) {
		Runtime run = Runtime.getRuntime();// 返回与当前 Java 应用程序相关的运行时对象
		try {
			Process p = run.exec(cmd);// 启动另一个进程来执行命令
			BufferedInputStream in = new BufferedInputStream(p.getInputStream());
			BufferedReader inBr = new BufferedReader(new InputStreamReader(in));
			String lineStr;
			while ((lineStr = inBr.readLine()) != null)
				// 获得命令执行后在控制台的输出信息
				System.out.println(lineStr);// 打印输出信息
			// 检查命令是否执行失败。
			if (p.waitFor() != 0) {
				if (p.exitValue() == 1)// p.exitValue()==0表示正常结束，1：非正常结束
					System.err.println("命令执行失败!");
			}
			inBr.close();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
