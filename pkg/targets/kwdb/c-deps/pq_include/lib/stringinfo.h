// Copyright (c) 2020-present,  INSPUR Co, Ltd.  All rights reserved.
/*-------------------------------------------------------------------------
 *
 * stringinfo.h
 *	  Declarations/definitions for "StringInfo" functions.
 *
 * StringInfo provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with palloc().
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/lib/stringinfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STRINGINFO_H
#define STRINGINFO_H

#define MaxAllocSize    ((size_t) 0x3fffffff) /* 1 gigabyte - 1 */

/*-------------------------
 * StringInfoData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with palloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *		cursor	is initialized to zero by makeStringInfo or initStringInfo,
 *				but is not otherwise touched by the stringinfo.c routines.
 *				Some routines use it to scan through a StringInfo.
 *-------------------------
 */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;


/*------------------------
 * There are two ways to create a StringInfo object initially:
 *
 * StringInfo stringptr = makeStringInfo();
 *		Both the StringInfoData and the data buffer are palloc'd.
 *
 * StringInfoData string;
 * initStringInfo(&string);
 *		The data buffer is palloc'd but the StringInfoData is just local.
 *		This is the easiest approach for a StringInfo object that will
 *		only live as long as the current routine.
 *
 * To destroy a StringInfo, pfree() the data buffer, and then pfree() the
 * StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using StringInfo, and then
 * release the StringInfoData but return the data string itself to their
 * caller.  At that point the data string looks like a plain palloc'd
 * string.
 *-------------------------
 */

/*------------------------
 * makeStringInfo
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
extern StringInfo makeStringInfo(void);

/*------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern int initStringInfo(StringInfo str);

/*------------------------
 * resetStringInfo
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 */
extern void resetStringInfo(StringInfo str);

/*------------------------
 * appendStringInfoString
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
extern int appendStringInfoString(StringInfo str, const char *s);

/*------------------------
 * appendStringInfoChar
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
extern int appendStringInfoChar(StringInfo str, char ch);

/*------------------------
 * appendStringInfoCharMacro
 * As above, but a macro for even more speed where it matters.
 * Caution: str argument will be evaluated multiple times.
 */
#define appendStringInfoCharMacro(str,ch) \
	(((str)->len + 1 >= (str)->maxlen) ? \
	 appendStringInfoChar(str, ch) : \
	 ((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'), STATUS_OK)

/*------------------------
 * appendStringInfoSpaces
 * Append a given number of spaces to str.
 */
extern int appendStringInfoSpaces(StringInfo str, int count);

/*------------------------
 * appendBinaryStringInfo
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
extern int appendBinaryStringInfo(StringInfo str,
					   const char *data, int datalen);

/*------------------------
 * enlargeStringInfo
 * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
 */
extern int enlargeStringInfo(StringInfo str, int needed);

#endif   /* STRINGINFO_H */
