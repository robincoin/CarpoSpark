package com.carpo.spark.utils;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public final class IOUtils {
    public static final byte[] NBYTE = new byte[0];

    public static final void loadProperties(final Properties props, final String filePath) {
        try {
            loadProperties(props, new FileInputStream(filePath));
        } catch (final Exception e) {
            e.printStackTrace();// for debug
        }
    }

    public static final void loadProperties(final Properties props, final InputStream inStream) {
        if (null == inStream)
            return;
        try {
            props.load(inStream);
        } catch (final Exception e) {
            e.printStackTrace();// for debug
        } finally {
            closeIO(inStream);
        }
    }

    /**
     * 用于过滤目录的文件过滤器
     */
    public static final FileFilter DIR_FILTER = new FileFilter() {
        public boolean accept(final File pathname) {
            return pathname.isDirectory();
        }
    };

    /**
     * 列取指定目录下的所有文件,加过滤器
     *
     * @return
     */
    public static List<File> listAllFiles(final File file, final FileFilter filter) {
        final List<File> filesList = new ArrayList<File>(8);
        if (file.isFile()) {
            filesList.add(file);
        } else {
            final File[] files = file.listFiles(filter);
            for (final File f : files) {
                filesList.addAll(listAllFiles(f, filter));
            }
        }
        return filesList;
    }

    public static void listAllFilesNoRs(final File file, final FileFilter filter) {
        if (file.isDirectory()) {
            final File[] files = file.listFiles(filter);
            for (final File f : files) {
                listAllFilesNoRs(f, filter);
            }
        }
    }


    /**
     * 安装关闭多个输入或者输出的流或者任何IO资源
     *
     * @param ioObjects
     */
    public static final void closeIO(final Closeable... ioObjects) {
        if (null == ioObjects || ioObjects.length == 0)//
            return;
        for (final Closeable ioObject : ioObjects) {
            try {
                if (null != ioObject)
                    ioObject.close();
            } catch (final Throwable e) {// ignore
            }
        }
    }

    /**
     * 用于解决MapBuf不释放文件句柄的问题,<a
     * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">bug
     * 4724038</a>
     *
     * @param buffer the Buffer to release
     */
    public static final void closeBuffer(final Buffer buffer) {
        if (null == buffer)
            return;
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    final Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                    if (null == cleanerMethod)
                        return null;
                    cleanerMethod.setAccessible(true);
                    final Object cleanerObj = cleanerMethod.invoke(buffer);
                    if (null == cleanerObj)
                        return null;
                    final Method cleanMethod = cleanerObj.getClass().getMethod("clean");
                    if (null == cleanMethod)
                        return null;
                    cleanMethod.invoke(cleanerObj);
                } catch (final Throwable e) {
                    // do nothing
                }
                return null;
            }
        });
    }

    /**
     * 创建一个空文件
     *
     * @param file
     * @throws IOException
     */
    public static void createFile(final File file) throws IOException {
        if (file.exists())
            return;
        if (file.getParentFile() != null)
            file.getParentFile().mkdirs();
        file.createNewFile();
    }

    /**
     * 将从输入流中读取数据并写入到输出流
     *
     * @param in
     * @param out
     * @return 整个过程拷贝的字节数据数
     * @throws IOException
     */
    public static int copyStream(final InputStream in, final OutputStream out) throws IOException {
        int readedCount = 0;
        final byte[] tmpBuf = new byte[1024];// 10K
        while (true) {
            final int numRead = in.read(tmpBuf);
            if (numRead == -1)
                break;
            out.write(tmpBuf, 0, numRead);
            out.flush();
            readedCount += numRead;
        }
        return readedCount;
    }

    public static int copyStream(final InputStream in, final ByteBuffer bb) throws IOException {
        int readedCount = 0;
        final byte[] tmpBuf = new byte[1024];// 10K
        while (true) {
            final int numRead = in.read(tmpBuf);
            if (numRead == -1)
                break;
            bb.put(tmpBuf, 0, numRead);
            bb.flip();
            readedCount += numRead;
        }
        return readedCount;
    }

    /**
     * 将给定输入数据流in写出到文件file中.
     *
     * @param file
     * @param in
     * @throws IOException
     */
    public static void writeToFile(final File file, final InputStream in) throws IOException {
        IOUtils.createFile(file);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            IOUtils.copyStream(in, fos);
        } finally {
            IOUtils.closeIO(fos);
        }
    }

    /**
     * 将给定输入数据b写出到文件file中.
     *
     * @param file
     * @param bytes
     * @throws IOException
     */
    public static void writeToFile(final File file, final byte[] bytes) throws IOException {
        IOUtils.createFile(file);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write(bytes);
            fos.flush();
        } finally {
            IOUtils.closeIO(fos);
        }
    }

    public static byte[] getStreamBytes(final InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyStream(in, out);
        return out.toByteArray();
    }

    public static byte[] getBytes(final Serializable o) throws IOException {
        return IOUtils.getBytes(o, true);
    }

    public static byte[] getBytes(final Serializable o, final boolean zip) throws IOException {
        if (o == null) {
            return IOUtils.NBYTE;
        }
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        if (zip) {
            final ByteArrayOutputStream ret = new ByteArrayOutputStream();
            final ZipOutputStream zos = new ZipOutputStream(ret);
            zos.putNextEntry(new ZipEntry("obj"));
            zos.write(bos.toByteArray());
            zos.close();
            return ret.toByteArray();
        }
        return bos.toByteArray();
    }

    public static Serializable unSerial(final byte[] b) throws Exception {
        return IOUtils.unSerial(b, true);
    }

    public static Serializable unSerial(final byte[] b, final boolean zip) throws Exception {
        if (b == null || b.length == 0) {
            return null;
        }
        final ByteArrayInputStream bis = new ByteArrayInputStream(b);
        if (zip) {
            ZipInputStream zis = null;
            try {
                zis = new ZipInputStream(bis);
                if (zis.getNextEntry() != null) {
                    int count;
                    final byte data[] = new byte[2048];
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    while ((count = zis.read(data, 0, 2048)) != -1) {
                        bos.write(data, 0, count);
                    }
                    final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
                    return (Serializable) ois.readObject();
                }
                return null;
            } finally {
                if (zis != null) {
                    zis.close();
                }
            }
        }
        final ObjectInputStream ois = new ObjectInputStream(bis);
        return (Serializable) ois.readObject();
    }

    /**
     * 快速拷贝文件的方法.
     *
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    public static void copyFile(final File sourceFile, final File destFile) throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }
        FileInputStream inStream = null;
        FileOutputStream outStream = null;
        FileChannel source = null;
        FileChannel destination = null;
        try {
            source = (inStream = new FileInputStream(sourceFile)).getChannel();
            destination = (outStream = new FileOutputStream(destFile)).getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            closeIO(source);
            closeIO(inStream);
            closeIO(destination);
            closeIO(outStream);
        }
    }

    /**
     * 快速拷贝一个目录结构
     *
     * @param sourceDir
     * @param destDir
     * @throws IOException
     */
    public static void copyDirectory(final File sourceDir, final File destDir) throws IOException {
        if (!destDir.exists()) {
            destDir.mkdir();
        }

        final File[] children = sourceDir.listFiles();
        for (final File sourceChild : children) {
            final String name = sourceChild.getName();
            final File destChild = new File(destDir, name);
            if (sourceChild.isDirectory()) {
                copyDirectory(sourceChild, destChild);
            } else {
                copyFile(sourceChild, destChild);
            }
        }
    }

    /**
     * 将一个文件的内容读取成一个字符串，如果出现任何异常将返回null
     *
     * @param file
     * @return
     */
    public static final String readFromFileAsString(final File file) {
        try {
            return new String(IOUtils.readFromFile(file));
        } catch (final Exception e) {
            return null;
        }
    }

    public static byte[] readFromFile(final File file) throws IOException {
        return IOUtils.getStreamBytes(new FileInputStream(file));
    }

    /**
     * 将一个给定的输入流读取成一个字符串
     *
     * @param inputStream
     * @return
     */
    public static final String readFromStreamAsString(final InputStream inputStream) {
        try {
            return new String(IOUtils.getStreamBytes(inputStream));
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * 从文件中读取一个字符串列表
     *
     * @param filename String
     * @return String[]
     */
    public static List<String> readListFromFile(final String filename) throws IOException {
        return readListFromFile(new File(filename));
    }

    public static List<String> readListFromFile(final InputStream stream) throws IOException {
        return readListFromFile(stream, null);
    }

    /**
     * @param file  要解析的文件
     * @param split 对每一行数据进行分隔的字符
     * @return 数据列表
     * @throws IOException
     */
    public static List<List<String>> readListFromFile(final File file, final String split) throws IOException {
        final List<String> lines = readListFromFile(file);
        final List<List<String>> datasList = new ArrayList<List<String>>();
        for (final String line : lines) {
            final List<String> dataList = new ArrayList<String>();
            dataList.addAll(Arrays.asList(line.split(split)));
            datasList.add(dataList);
        }
        return datasList;
    }

    public static List<String> readListFromFile(final InputStream stream, final String[] exceptStartwith) throws IOException {
        final List<String> result = new ArrayList<String>();
        final BufferedReader bufferedreader = new BufferedReader(new InputStreamReader(stream));
        String s;
        try {
            while ((s = bufferedreader.readLine()) != null) {
                if (exceptStartwith != null) {
                    boolean contain = false;
                    for (final String except : exceptStartwith) {
                        if (s.startsWith(except)) {
                            contain = true;
                            break;
                        }
                    }
                    if (contain)
                        continue;
                }
                result.add(s);
            }
        } catch (Exception e) {
        } finally {
            closeIO(stream);
        }

        return result;
    }

    public static List<String> readListFromFile(final File file) throws IOException {
        return readListFromFile(new FileInputStream(file), null);
    }

    public static List<String> readListFromFile(final File file, final String[] except) throws IOException {
        return readListFromFile(new FileInputStream(file), except);
    }

    /**
     * 安全删除一个文件或者目录树
     *
     * @param file
     * @see #delete(File)
     */
    public static final void delete(final String file) {
        delete(new File(file));
    }

    /**
     * 安全删除一个文件或者目录树
     *
     * @param fileOrDir
     */
    public static final void delete(final File fileOrDir) {
        clearDir(fileOrDir);
        fileOrDir.delete();
        fileOrDir.deleteOnExit();
    }

    /**
     * 清空目录
     *
     * @param fileOrDir
     */
    public static final void clearDir(final File fileOrDir) {
        if (!fileOrDir.exists())
            return;
        if (fileOrDir.isFile()) {
            fileOrDir.delete();
            fileOrDir.deleteOnExit();
            return;
        }
        final File subFiles[] = fileOrDir.listFiles();
        if (null != subFiles && subFiles.length > 0) {
            for (final File subFile : subFiles)
                delete(subFile);
        }
    }

    public static String getFileNameWithoutExtendsion(final String s) {
        String ext = null;
        final int i = s.lastIndexOf('.');

        if (i > 0 && i < s.length() - 1) {
            ext = s.substring(0, i);
        }
        return ext;
    }

    public static final String read(final InputStream is, final int length) throws Exception {
        final byte[] buf = new byte[length];
        is.read(buf);
        return new String(buf);
    }


    public static boolean validateExtension(final File file, final String... exten) {
        if (exten == null) {
            return true;
        }
        final String fName = file.getName();
        final String extension = fName.substring(fName.lastIndexOf("."), fName.length()).toLowerCase();
        for (final String ext : exten)
            if (ext.toLowerCase().endsWith(extension))
                return true;
        return false;
    }

    /**
     * 根据文件字节长度显示标签,如:1KB,1MB,1GB,
     *
     * @param fileSize
     * @return
     */
    public static final String getFileSizeLabel(final long fileSize) {
        double label;
        String danWei;
        final long kb = 1024;
        if (fileSize < kb)
            return fileSize + "Byte";
        final long mb = kb * 1024;
        final long gb = mb * 1024;

        if (fileSize < mb) {
            label = (double) fileSize / (double) kb;
            danWei = "KB";
        } else if (fileSize < gb) {
            label = (double) fileSize / (double) mb;
            danWei = "MB";
        } else {
            label = (double) fileSize / (double) gb;
            danWei = "GB";
        }
        return NumberFormat.getNumberInstance().format(label) + danWei;
    }

    /**
     * @param out         输出打印接口
     * @param indentLevel 每一个单位对应2个空格位置
     * @param args        要输出的数据集
     */
    public static final void println(final PrintStream out, final int indentLevel, final Object... args) {
        final int j = indentLevel * 2;
        int i = 0;
        final StringBuilder buf = new StringBuilder(32);
        do {
            if (i >= j)
                break;
            buf.append(' ');
            i++;
        } while (true);
        for (final Object obj : args)
            buf.append(obj);
        out.println(buf.toString());
    }


    /**
     * 获得该磁盘的剩余空间
     *
     * @param file
     * @return
     */
    public static int getFreeSpaceM(final File file) {
        if (!file.exists()) {
            file.mkdirs();
        }
        return (int) (file.getFreeSpace() / (1024 * 1024));
    }

}
