import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlLog {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printHelpInfo();
            return;
        }
        parseArgs(args);
        parseSql2();
        work();
    }

    public static void printHelpInfo() {
        String msg = "";
        msg += "format is : java SqlLog [options] \"sql\"";
        msg += "\nsql sample : java SqlLog \"select 'regx with group(num\\d),(\\d)' 'format "
                + "\nstring:val1=${1},val2=count(${2})' " + "from 'filename1','filename2' "
                + "\nin path local://local/Library/morePath1,local://local/Library/morePath2"
                + "\nwhere match 'regx' and not match 'regx'"
                + "\nand #1=0 and #2<=5 and #2 in (1,2,3) " + "order by #1,#2 " + "group by #1,#2 "
                + "\nlimit 1,2 " + "\"";
        msg += "\n[options]";

        msg += "\n[-follow]:will implememt as tail -f filename,follow file change";

        msg += "\n[-ignoreCase]:will implememt as grep -i for where condition (match regx , not "
                + "match regx) ";

        msg += "\n[-maxLine=10000]: group by and order by option need cache data in memory ,this "
                + "option "
                + "limit max cache data line count,if over maxLine will throw Exception";

        msg += "\n[-preLimit=10,5]:means skip 10 input lines and allow 5 line input follow ";

        msg += "\n[-tail]：normally we read file from head to end ,this whill read file from end to "
                + "head ,implement by cmd tac";
        out(msg);

    }

    public static void parseArgs(String[] args) {
        //预处理参数，拆分元素，去空格
        List<String> argList = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (i == args.length - 1) {
                argList.add(args[i].trim());
                break;
            }
            for (String line : args[i].split(" ")) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                argList.add(line.trim());
            }
        }
        args = argList.toArray(new String[] {});

        log(args[args.length - 1]);
        for (int i = 0; i < args.length; i++) {
            String para = args[i];
            if (para.startsWith("-debug")) {
                ParsedSql.isDebug = true;
            }
            log("arg" + i + ":" + args[i]);

            if (para.startsWith("-follow")) { //tail -f
                ParsedSql.isFollow = true;
            }
            if (para.startsWith("-ignoreCase")) { //grep -i
                ParsedSql.isIgnoreCase = true;
            }
            if (para.startsWith("-maxLine")) { //最大内存处理行数，默认1w
                ParsedSql.maxLine = Integer.parseInt(para.split("=")[1]);
            }
            if (para.startsWith("-preLimit")) { //-preLimit=2,3 跳过2行，然后取随后3行
                //tail -f 模式不要用这个配置，会导致攒够limit数据
                ParsedSql.preSkipLine = Integer.parseInt(para.split("=")[1].split(",")[0]);
                ParsedSql.preLimitLine = Integer.parseInt(para.split("=")[1].split(",")[1]);
                ParsedSql.isPreLimitLine = true;
            }
            if (para.startsWith("-tail")) { //tac
                ParsedSql.isTail = true;
            }
            if (i == args.length - 1) { //最后一个是sql
                ParsedSql.rowSql = para;
            }

            if (ParsedSql.isFollow && ParsedSql.isPreLimitLine) {
                throw new RuntimeException("-follow and -preLimit can not set at the same time");
            }
            if (ParsedSql.isTail && ParsedSql.isFollow) {
                throw new RuntimeException("-follow and -tail can not set at the same time");
            }
        }
    }

    public static void parseSql2() {

        //过滤注释
        StringBuilder sb = new StringBuilder();
        for (String line : ParsedSql.rowSql.split("\n")) {
            line = line.trim();
            if (line.startsWith("//") || line.startsWith("#")) {
                log("skip comment :" + line);
                continue;
            }
            sb.append(line).append(" ");
        }
        String sql = sb.toString();

        ParsedSql.ogi = ParseTree.toBasicStr(sql);
        ParsedSql.selects = new ArrayList<>();
        ParsedSql.froms = new ArrayList<>();
        ParsedSql.inPaths = new ArrayList<>();
        ParsedSql.wheres = new ArrayList<>();
        ParsedSql.orderBys = new ArrayList<>();
        ParsedSql.groupBys = new ArrayList<>();
        ParsedSql.limits = new ArrayList<>();
        List<String> cur = new ArrayList<>();
        for (int i = 0; i < ParsedSql.ogi.size(); i++) {
            String elm = ParsedSql.ogi.get(i);
            if (elm.equals("select")) {
                cur = ParsedSql.selects;
                continue;
            }
            if (elm.equals("from")) {
                cur = ParsedSql.froms;
                continue;
            }
            if (elm.equals("in") && ParsedSql.ogi.get(i + 1).equals("path")) {
                cur = ParsedSql.inPaths;
                i++;
                continue;
            }
            if (elm.equals("where")) {
                cur = ParsedSql.wheres;
                continue;
            }
            if (elm.equals("order") && ParsedSql.ogi.get(i + 1).equals("by")) {
                cur = ParsedSql.orderBys;
                i++;

                continue;
            }
            if (elm.equals("group") && ParsedSql.ogi.get(i + 1).equals("by")) {
                cur = ParsedSql.groupBys;
                i++;
                continue;
            }
            if (elm.equals("limit")) {
                cur = ParsedSql.limits;
                continue;
            }
            cur.add(elm);
        }

        if (ParsedSql.selects.size() == 1) {
            if (ParsedSql.selects.get(0).equals("*")) {
                ParsedSql.selects = new ArrayList<>();
                ParsedSql.selects.add("(.*)");
                ParsedSql.selects.add("#{0}");
            }
            if (ParsedSql.selects.get(0).equals("count(*)")) {
                ParsedSql.selects = new ArrayList<>();
                ParsedSql.selects.add("(.*)");
                ParsedSql.selects.add("count(#{1})");
            }
        }
        String formatStr = ParsedSql.selects.get(1);
        if (Util.canFind("count\\(\\#\\{\\d*?\\}\\)", formatStr)
                || Util.canFind("sum\\(#\\{\\d+?\\}\\)", formatStr)
                || Util.canFind("max\\(\\#\\{\\d*?\\}\\)", formatStr)
                || Util.canFind("min\\(\\#\\{\\d*?\\}\\)", formatStr)
                || Util.canFind("avg\\(\\#\\{\\d*?\\}\\)", formatStr)) {
            ParsedSql.isGroup = true;
        }

        ParsedSql.ands = Util.splitByElm(ParsedSql.wheres, "and");
        log("--------------");
        log(ParsedSql.ogi);
        log("--------------");
        log("len=" + ParsedSql.selects.size() + "|||" + ParsedSql.selects);
        log("--------------");

        log(ParsedSql.froms);
        log(ParsedSql.inPaths);
        log(ParsedSql.wheres);
        log(ParsedSql.orderBys);
        log(ParsedSql.groupBys);
        log(ParsedSql.limits);

        String from = ParsedSql.froms.get(0);
        ParsedSql.isPipe = from.equals("pipe");
        if (!ParsedSql.isPipe) {
            //例如 main.log,main.log.2019-01-03
            ParsedSql.files = Util.removeCommon(ParsedSql.froms).toArray(new String[] {});

            //例如file://host/data/logs,ssh://host/data/logs/proj,sshpass://host/data/logs/proj
            if (ParsedSql.inPaths.get(0).startsWith("local")) {
                ParsedSql.protocal = "local";
            } else if (ParsedSql.inPaths.get(0).startsWith("sshpass")) {
                ParsedSql.protocal = "sshpass";
            } else {
                ParsedSql.protocal = "ssh";
            }
            ParsedSql.paths = Util.splitByElm(ParsedSql.inPaths, ",").stream()
                    .map(e -> e.get(0).replace("ssh://", "").replace("local://", "")
                            .replace("sshpass://", "")) //
                    .collect(Collectors.toSet()).toArray(new String[] {});

        }

        if (ParsedSql.limits.size() > 0) {
            if (ParsedSql.limits.size() == 3) {
                ParsedSql.limitStart = Integer.parseInt(ParsedSql.limits.get(0));
                ParsedSql.limitLine = Integer.parseInt(ParsedSql.limits.get(2));
            } else {
                ParsedSql.limitLine = Integer.parseInt(ParsedSql.limits.get(0));
            }
        }
    }

    public static void log(Object x) {
        if (ParsedSql.isDebug) {
            System.out.println(x);
        }
    }

    public static void out(Object x) {
        System.out.println(x);
    }

    public static void exec(String[] cmd, Consumer<String> lineHandler) throws IOException {
        Process process = Runtime.getRuntime().exec(cmd);
        LineNumberReader br = new LineNumberReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = br.readLine()) != null) {
            lineHandler.accept(line);
        }
    }

    public static void work() throws Exception {
        if (ParsedSql.isPipe) {
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.next();
                dealLine(line);
            }
        } else {
            List<String> parsedInnerCmdStr = new ArrayList<>();

            String preInnerCmd = "";
            if (ParsedSql.isFollow) {
                preInnerCmd += "tail -f ";
            } else if (ParsedSql.isTail) {
                if (Util.isMac()) {
                    preInnerCmd += "tail -r ";
                } else {
                    preInnerCmd += "tac ";
                }
            } else {
                preInnerCmd += "cat ";
            }

            //这里的思路是尽可能在远程过滤数据，以减少传输数据耗时
            String postInnerCmd = "";
            if (ParsedSql.isPreLimitLine) {
                postInnerCmd += " | head -n " + (ParsedSql.preLimitLine + ParsedSql.preSkipLine)
                        + " | tail -n +" + (ParsedSql.preSkipLine + 1) + " ";
            }

            String ignore = ParsedSql.isIgnoreCase ? " -i " : "";
            for (List<String> exp : ParsedSql.ands) {
                if (Util.isMatch(exp)) {
                    postInnerCmd += " | grep --line-buffer " + ignore + "-e \'" + exp.get(1)
                            + "\' ";
                }
                if (Util.isNoMatch(exp)) {
                    postInnerCmd += " | grep --line-buffer " + ignore + "-v -e \'" + exp.get(2)
                            + "\' ";
                }
            }

            for (int i = 0; i < ParsedSql.paths.length; i++) {
                for (int j = 0; j < ParsedSql.files.length; j++) {
                    String url = ParsedSql.paths[i];
                    String host = url.split("/")[0];
                    String path = url.replace(host, "");
                    String file = ParsedSql.files[j];
                    String fullPath = path + "/" + file;

                    String innerCmdStr = "";
                    if (ParsedSql.isTail) {
                        innerCmdStr += preInnerCmd + fullPath;
                    } else {
                        innerCmdStr += preInnerCmd + fullPath;
                    }

                    if (ParsedSql.protocal.equals("sshpass")) {
                        innerCmdStr = " sshpass -p yourpassword ssh yourAccount@" + host + " '"
                                + innerCmdStr + "' ";
                    } else if (ParsedSql.protocal.equals("ssh")) {
                        innerCmdStr = "ssh " + host + " '" + innerCmdStr + "' ";
                    }

                    innerCmdStr += postInnerCmd;
                    parsedInnerCmdStr.add(innerCmdStr);
                }
            }

            String[] cmdBase = new String[] { "/bin/sh", "-c", "" };

            if (ParsedSql.isFollow && parsedInnerCmdStr.size() != 1) {
                throw new RuntimeException("follow mode only follow one path");
            }
            for (String cmd : parsedInnerCmdStr) {
                cmdBase[2] = cmd;
                log("------run:" + Arrays.toString(cmdBase));
                exec(cmdBase, line -> {
                    dealLine(line);
                });
            }

        }
        dealCachedOpt();

        if (haveCachedOpt()) {
            log("---final output----");

            if (ParsedSql.groupLines.size() > 0) {
                ParsedSql.groupLines.values().forEach(groupLine -> {
                    String formatStr = ParsedSql.selects.get(1);

                    for (int fmtIdx = 1; fmtIdx <= ParsedSql.fmtParaSize; fmtIdx++) {
                        if ("smp".equals(ParsedSql.fmtParaInfo[fmtIdx][1])) {
                            formatStr = formatStr.replaceAll(
                                    "\\#\\{" + ParsedSql.fmtParaInfo[fmtIdx][0] + "\\}",
                                    groupLine[ParsedSql.paraSize + fmtIdx]);
                        } else {
                            String repStr = ParsedSql.fmtParaInfo[fmtIdx][1] + "\\(\\#\\{"
                                    + ParsedSql.fmtParaInfo[fmtIdx][0] + "\\}\\)";
                            String toStr = groupLine[ParsedSql.paraSize + fmtIdx];
                            formatStr = formatStr.replaceAll(repStr, toStr);
                        }

                    }
                    formatStr = formatStr.replaceAll("\\#\\{" + 0 + "\\}", groupLine[0]);
                    out(formatStr);

                });
            } else {
                ParsedSql.parsedLines.forEach(SqlLog::outputOneLine);
            }
        }

    }

    public static void dealLine(String line) {
        ParsedSql.rowLineCount++;
        if (!preDealLine(line)) {
            return;
        }
        String[] parsedLine = skipAndParseLine(line);

        if (parsedLine.length > 0 && !haveCachedOpt()) {
            outputOneLine(parsedLine);
        }
    }

    public static void outputOneLine(String[] parsedLine) {
        String formatStr = ParsedSql.selects.get(1);
        for (int j = 0; j <= ParsedSql.paraSize; j++) {
            formatStr = formatStr.replaceAll("\\#\\{" + j + "\\}", parsedLine[j]);
        }
        out(formatStr);
    }

    public static boolean haveCachedOpt() {
        return ParsedSql.orderBys.size() > 0 || hasGroup();
    }

    //处理需要缓存的操作，主要是group，order
    public static void dealCachedOpt() {

        if (ParsedSql.orderBys.size() > 0) {
            List<List<String>> spliList = Util.splitByElm(ParsedSql.orderBys, ",");

            //            List<String> cleanKeys = Util.removeCommon(ParsedSql.orderBys);
            List<Integer> keyIndesx = spliList.stream() //
                    .map(e -> Util.getVarNum(e.get(0))) //
                    .collect(Collectors.toList());
            List<Boolean> isAscList = spliList.stream()
                    .map(e -> e.size() != 2 || !e.get(1).equals("desc"))
                    .collect(Collectors.toList());
            int keyNum = keyIndesx.size();
            Comparator<String[]> comparator = (line1, line2) -> {
                for (int i = 0; i < keyNum; i++) {
                    int idx = keyIndesx.get(i);

                    BigDecimal num1 = Util.toBigDecimal(line1[idx]);
                    if (num1 != null) {
                        BigDecimal num2 = Util.toBigDecimal(line2[idx]);
                        int ret;
                        if (isAscList.get(i)) {
                            ret = num1.compareTo(num2);
                        } else {
                            ret = num2.compareTo(num1);
                        }
                        if (ret != 0) {
                            return ret;
                        }
                        continue;
                    }

                    //否则字符串比较
                    int ret;
                    if (isAscList.get(i)) {
                        ret = line1[idx].compareTo(line2[idx]);
                    } else {
                        ret = line2[idx].compareTo(line1[idx]);
                    }
                    if (ret != 0) {
                        return ret;
                    }
                    continue;
                }
                return 0;
            };

            ParsedSql.parsedLines = ParsedSql.parsedLines.stream().sorted(comparator)
                    .collect(Collectors.toList());
        }

        if (hasGroup()) {
            List<String> cleanKeys = Util.removeCommon(ParsedSql.groupBys);
            List<Integer> groupByKeyIndex = cleanKeys.stream() //
                    .map(e -> Util.getVarNum(e)).collect(Collectors.toList());
            int paraSize = ParsedSql.paraSize;
            int groupLineSize = paraSize + 1 + ParsedSql.fmtParaSize + 1;
            int lastGroupIdx = groupLineSize - 1;
            for (int i = 0; i < ParsedSql.parsedLines.size(); i++) {
                String[] line = ParsedSql.parsedLines.get(i);
                String key = "_";
                for (Integer idx : groupByKeyIndex) {
                    key += line[idx] + "_";
                }
                String[] groupLine = ParsedSql.groupLines.get(key);
                if (groupLine == null) {
                    groupLine = Arrays.copyOf(line, groupLineSize);

                    for (int fmtIdx = 1; fmtIdx <= ParsedSql.fmtParaSize; fmtIdx++) {
                        String groupType = ParsedSql.fmtParaInfo[fmtIdx][1];
                        if ("count".equals(groupType) || "sum".equals(groupType)
                                || "avg".equals(groupType)) {
                            groupLine[fmtIdx + paraSize] = "0";
                        } else {
                            groupLine[fmtIdx + paraSize] = line[Integer
                                    .parseInt(ParsedSql.fmtParaInfo[fmtIdx][0])];
                        }
                    }
                    groupLine[lastGroupIdx] = "0"; //count
                    ParsedSql.groupLines.put(key, groupLine);
                }
                groupLine[lastGroupIdx] = new BigDecimal(groupLine[lastGroupIdx])
                        .add(BigDecimal.ONE).toString();
                for (int fmtIdx = 1; fmtIdx <= ParsedSql.fmtParaSize; fmtIdx++) {

                    if ("count".equals(ParsedSql.fmtParaInfo[fmtIdx][1])) {
                        groupLine[fmtIdx + paraSize] = groupLine[lastGroupIdx];
                    }
                    if ("max".equals(ParsedSql.fmtParaInfo[fmtIdx][1])) {
                        if (new BigDecimal(groupLine[fmtIdx + paraSize]).compareTo(new BigDecimal(
                                line[Integer.parseInt(ParsedSql.fmtParaInfo[fmtIdx][0])])) < 0) {
                            groupLine[fmtIdx + paraSize] = line[Integer
                                    .parseInt(ParsedSql.fmtParaInfo[fmtIdx][0])];
                        }
                    }
                    if ("min".equals(ParsedSql.fmtParaInfo[fmtIdx][1])) {
                        if (new BigDecimal(groupLine[fmtIdx + paraSize]).compareTo(new BigDecimal(
                                line[Integer.parseInt(ParsedSql.fmtParaInfo[fmtIdx][0])])) > 0) {
                            groupLine[fmtIdx + paraSize] = line[Integer
                                    .parseInt(ParsedSql.fmtParaInfo[fmtIdx][0])];
                        }
                    }
                    if ("sum".equals(ParsedSql.fmtParaInfo[fmtIdx][1])
                            || "avg".equals(ParsedSql.fmtParaInfo[fmtIdx][1])) {
                        groupLine[fmtIdx + paraSize] = new BigDecimal(
                                line[Integer.parseInt(ParsedSql.fmtParaInfo[fmtIdx][0])])
                                        .add(new BigDecimal(groupLine[fmtIdx + paraSize]))
                                        .toString();
                    }
                }

            } //end for

            ParsedSql.groupLines.values().forEach(groupLine -> {
                for (int fmtIdx = 1; fmtIdx <= ParsedSql.fmtParaSize; fmtIdx++) {
                    if ("avg".equals(ParsedSql.fmtParaInfo[fmtIdx][1])) {
                        groupLine[fmtIdx + paraSize] = new BigDecimal(groupLine[fmtIdx + paraSize])
                                .divide(new BigDecimal(groupLine[lastGroupIdx]), 2,
                                        BigDecimal.ROUND_HALF_EVEN)
                                .toString();
                    }
                }
            });
        }
    }

    public static boolean hasGroup() {
        return ParsedSql.groupBys.size() > 0 || ParsedSql.isGroup;
    }

    //对于管道模式，在这里实现cmd实现的命令
    public static boolean preDealLine(String line) {

        line = line.trim();
        if (ParsedSql.isPipe) {

            if (ParsedSql.rowLineCount <= ParsedSql.preSkipLine) {
                return false;
            }
            if (ParsedSql.rowLineCount > ParsedSql.preLimitLine) {
                return false;
            }

            for (List<String> exp : ParsedSql.ands) {
                if (Util.isEmpty(line)) {
                    return false;
                }

                if (Util.isMatch(exp)) {
                    if (!Util.canFind(exp.get(1), line)) {
                        return false;
                    }
                }
                if (Util.isNoMatch(exp)) {

                    if (Util.canFind(exp.get(2), line)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static void lasyInitParaType2(int paraCount) {
        String formatStr = ParsedSql.selects.get(1);
        ParsedSql.paraSize = paraCount;
        if (ParsedSql.fmtParaInfo == null) { //这个map存放
            int fmtParaIdx = 1;
            ParsedSql.fmtParaInfo = new String[500][2]; //最大支持500个格式化参数
            for (int paraIdx = 1; paraIdx <= ParsedSql.paraSize; paraIdx++) {
                if (formatStr.contains("count(#{" + paraIdx + "})")) {
                    ParsedSql.fmtParaInfo[fmtParaIdx++] = new String[] { paraIdx + "", "count" };
                }
                if (formatStr.contains("sum(#{" + paraIdx + "})")) {
                    ParsedSql.fmtParaInfo[fmtParaIdx++] = new String[] { paraIdx + "", "sum" };
                }
                if (formatStr.contains("max(#{" + paraIdx + "})")) {
                    ParsedSql.fmtParaInfo[fmtParaIdx++] = new String[] { paraIdx + "", "max" };
                }
                if (formatStr.contains("min(#{" + paraIdx + "})")) {
                    ParsedSql.fmtParaInfo[fmtParaIdx++] = new String[] { paraIdx + "", "min" };
                }
                if (formatStr.contains("avg(#{" + paraIdx + "})")) {
                    ParsedSql.fmtParaInfo[fmtParaIdx++] = new String[] { paraIdx + "", "avg" };
                }
                if (formatStr.contains("#{" + paraIdx + "}")) {
                    ParsedSql.fmtParaInfo[fmtParaIdx++] = new String[] { paraIdx + "", "smp" };
                }
            }
            ParsedSql.fmtParaSize = fmtParaIdx - 1;

        }

    }

    public static String[] skipAndParseLine(String line) {

        String regStr = ParsedSql.selects.get(0);
        Pattern ptn = Pattern.compile(regStr);
        Matcher match = ptn.matcher(line);
        if (!match.find()) {
            return new String[0];
        }
        //        lasyInitParaType(match.groupCount());
        lasyInitParaType2(match.groupCount());

        String[] findPara = new String[match.groupCount() + 1];
        for (int i = 0; i < findPara.length; i++) {
            findPara[i] = match.group(i);
        }

        if (!isMatchWhere(findPara)) {
            return new String[0];
        }

        ParsedSql.curLine++;
        if (ParsedSql.curLine <= ParsedSql.limitStart) {
            return new String[0];
        }
        if (ParsedSql.curLine > ParsedSql.limitStart + ParsedSql.limitLine) {
            return new String[0];
        }
        if (haveCachedOpt()) {
            ParsedSql.parsedLines.add(findPara);
            if (ParsedSql.parsedLines.size() > ParsedSql.maxLine) {
                throw new RuntimeException("over max line=" + ParsedSql.maxLine);
            }
        }
        SqlLog.log(line);
        return findPara;
    }

    public static boolean isMatchWhere(String[] paras) {
        for (List<String> exp : ParsedSql.ands) {

            if (Util.isEq(exp)) {
                if (!paras[Util.getVarNum(exp.get(0))].equals(exp.get(2))) {
                    return false;
                }
            }
            if (Util.isNotEq(exp)) {
                if (paras[Util.getVarNum(exp.get(0))].equals(exp.get(2))) {
                    return false;
                }
            }
            if (Util.isGt(exp)) {
                if (Util.compare(paras[Util.getVarNum(exp.get(0))], exp.get(2)) <= 0) {
                    return false;
                }
            }
            if (Util.isGe(exp)) {
                if (Util.compare(paras[Util.getVarNum(exp.get(0))], exp.get(2)) < 0) {
                    return false;
                }
            }
            if (Util.isLs(exp)) {
                if (Util.compare(paras[Util.getVarNum(exp.get(0))], exp.get(2)) >= 0) {
                    return false;
                }
            }
            if (Util.isLe(exp)) {
                if (Util.compare(paras[Util.getVarNum(exp.get(0))], exp.get(2)) > 0) {
                    return false;
                }
            }
            if (Util.isIn(exp)) {
                List<String> ids = Util.removeCommon(exp.subList(3, exp.size() - 1));
                boolean isIn = false;
                for (String id : ids) {
                    if (paras[Util.getVarNum(exp.get(0))].equals(id)) {
                        isIn = true;
                    }
                }
                if (!isIn) {
                    return false;
                }
            }
            if (Util.isNotIn(exp)) {
                List<String> ids = Util.removeCommon(exp.subList(4, exp.size() - 1));
                for (String id : ids) {
                    if (paras[Util.getVarNum(exp.get(0))].equals(id)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    static class ParsedSql {

        static List<String> ogi;
        static List<String> selects = new ArrayList<>();
        static List<String> froms = new ArrayList<>();
        static List<String> inPaths = new ArrayList<>();
        static List<String> wheres = new ArrayList<>();
        static List<String> orderBys = new ArrayList<>();
        static List<String> groupBys = new ArrayList<>();
        static List<String> limits = new ArrayList<>();

        static List<List<String>> ands = new ArrayList<>();

        static List<String[]> parsedLines = new ArrayList<>();
        static Map<String, String[]> groupLines = new HashMap<>();
        static int paraSize;
        static boolean isGroup = false;

        static String[][] fmtParaInfo = null; //索引1是fmt_para_index,2-0是ogi_para_idx,2-1是组函数类型
        static int fmtParaSize;

        static String[] files;
        static String protocal;
        static String[] paths;
        static int limitLine = Integer.MAX_VALUE;
        static int limitStart = 0;
        static int curLine = 0;
        static int rowLineCount = 0;

        //--cfg--//
        static int maxLine = 100000; //如果有内存积累数据的运算，最大缓存这么多条
        static int preSkipLine = 0; //跳过若干原始行
        static int preLimitLine = Integer.MAX_VALUE; //处理若干原始行
        static boolean isPreLimitLine = false;
        static boolean isPipe = true;
        static boolean isTail = false;
        static boolean isDebug = false;
        static boolean isFollow = false;
        static boolean isIgnoreCase = false;
        //--data--//
        static String rowSql;
    }

    static class ParseTree {

        enum STAT {
            SPACE, QUOT, NORM
        }

        static char beforeChar = ' ';
        static Set<Character> codes;

        public static List<String> toBasicStr(String sql) {
            List<String> ls = new ArrayList<>();
            String trimStr = sql.trim();
            char[] carr = trimStr.toCharArray();
            String tmp = "";
            STAT stat = STAT.SPACE;
            for (int i = 0; i < carr.length; i++) {
                char cc = carr[i];
                beforeChar = i > 0 ? carr[i - 1] : ' ';
                if (stat.equals(STAT.SPACE)) {
                    if (isComm(cc)) {
                        ls.add("" + cc);
                        continue;
                    }
                    if (isSpace(cc)) {
                        continue;
                    }
                    if (isQuot(cc)) {
                        stat = STAT.QUOT;
                        continue;
                    }
                    stat = STAT.NORM;
                    tmp += cc;
                    continue;
                }

                if (stat.equals(STAT.QUOT)) {
                    if (isQuot(cc)) {
                        stat = STAT.SPACE;
                        ls.add(tmp);
                        tmp = "";
                        continue;
                    }
                    tmp += cc;
                    continue;
                }

                if (stat.equals(STAT.NORM)) {
                    if (isSpace(cc)) {
                        stat = STAT.SPACE;
                        ls.add(tmp);
                        tmp = "";
                        continue;
                    }
                    if (isComm(cc)) {
                        stat = STAT.SPACE;
                        ls.add(tmp);
                        tmp = "";
                        ls.add("" + cc);
                        continue;
                    }
                    tmp += cc;
                    continue;
                }

            }
            if (!tmp.isEmpty()) {
                ls.add(tmp);
            }
            return connectTuple(ls);
        }

        public static boolean isSpace(char c) {
            return ' ' == c;
        }

        public static boolean isComm(char c) {

            if (codes == null) {
                char[] specialCode = "()[]<>,.?!;~+-*%&|^=".toCharArray();
                codes = new HashSet<>();
                for (int i = 0; i < specialCode.length; i++) {
                    codes.add(specialCode[i]);
                }
            }
            return codes.contains(c);
        }

        //对于二元或者n元运算符，做一下连接
        //包括：  >=,<=,
        public static List<String> connectTuple(List<String> ls) {
            List<String> ls2 = new ArrayList<>();

            for (int i = 0; i < ls.size(); i++) {
                if (i + 1 == ls.size()) {
                    ls2.add(ls.get(i));
                    break;
                }
                String elm = ls.get(i);
                if (elm.equals(">") && ls.get(i + 1).equals("=")) {
                    ls2.add(">=");
                    i++;
                    continue;
                }
                if (elm.equals("<") && ls.get(i + 1).equals("=")) {
                    ls2.add("<=");
                    i++;
                    continue;
                }

                if (elm.equals("!") && ls.get(i + 1).equals("=")) {
                    ls2.add("!=");
                    i++;
                    continue;
                }

                ls2.add(elm);
            }
            return ls2;
        }

        public static boolean isQuot(char c) {
            return '\'' == c && beforeChar != '\\';
        }

    }

    static class Util {

        public static List<String> removeCommon(List<String> ls) {
            return ls.stream().filter(e -> !e.equals(",")).collect(Collectors.toList());
        }

        public static List<List<String>> splitByElm(List<String> ls, String splitor) {
            List<List<String>> res = new ArrayList<>();
            List<String> tls = new ArrayList<>();
            for (String e : ls) {
                if (e.equals(splitor)) {
                    res.add(tls);
                    tls = new ArrayList<>();
                    continue;
                }
                tls.add(e);
            }
            if (tls.size() > 0) {
                res.add(tls);
            }
            return res;
        }

        public static boolean isMatch(List<String> elms) {
            return elms.get(0).equals("match");
        }

        public static boolean isNoMatch(List<String> elms) {
            return elms.get(0).equals("not") && elms.get(1).equals("match");
        }

        public static boolean isEq(List<String> elms) {
            return elms.get(1).equals("=");
        }

        public static boolean isNotEq(List<String> elms) {
            return elms.get(1).equals("!=");
        }

        public static boolean isGt(List<String> elms) {
            return elms.get(1).equals(">");
        }

        public static boolean isGe(List<String> elms) {
            return elms.get(1).equals(">=");
        }

        public static boolean isLe(List<String> elms) {
            return elms.get(1).equals("<=");
        }

        public static boolean isLs(List<String> elms) {
            return elms.get(1).equals("<");
        }

        public static boolean isIn(List<String> elms) {
            return elms.get(1).equals("in");
        }

        public static boolean isNotIn(List<String> elms) {
            return elms.get(1).equals("not") && elms.get(2).equals("in");
        }

        public static boolean isEmpty(String str) {
            return str != null && str.isEmpty();
        }

        public static int compare(String x1, String x2) {
            return new BigDecimal(x1).compareTo(new BigDecimal(x2));
        }

        public static BigDecimal toBigDecimal(String data) {
            try {
                return new BigDecimal(data);
            } catch (Exception e) {
                return null;
            }
        }

        public static Integer getVarNum(String token) {
            return Integer.parseInt(token.replace("#{", "") //
                    .replace("#", "") //
                    .replace("}", ""));
        }

        public static boolean isMac() {
            return System.getProperty("os.name").startsWith("Mac OS");
        }

        public static boolean canFind(String regx, String str) {
            return Pattern.compile(regx).matcher(str).find();
        }
    }
}
