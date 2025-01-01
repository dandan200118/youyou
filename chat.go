package you

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bincooo/emit.io"
	_ "github.com/gingfrederik/docx"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Message struct {
	Answer   string `json:"answer"`
	Question string `json:"question"`
}

type Chat struct {
	cookie,
	clearance,
	mode,
	model,
	proxies string

	limitWithE bool

	session   *emit.Session
	userAgent string
	lang      string
}

const (
	GPT_4          = "gpt_4"
	GPT_4o         = "gpt_4o"
	GPT_4o_MINI    = "gpt_4o_mini"
	GPT_4_TURBO    = "gpt_4_turbo"
	OPENAI_O1      = "openai_o1"
	OPENAI_O1_MINI = "openai_o1_mini"

	CLAUDE_2          = "claude_2"
	CLAUDE_3_HAIKU    = "claude_3_haiku"
	CLAUDE_3_SONNET   = "claude_3_sonnet"
	CLAUDE_3_5_SONNET = "claude_3_5_sonnet"
	CLAUDE_3_OPUS     = "claude_3_opus"

	GEMINI_1_0_PRO   = "gemini_pro"
	GEMINI_1_5_PRO   = "gemini_1_5_pro"
	GEMINI_1_5_FLASH = "gemini_1_5_flash"
)

func New(cookie, model, proxies string) Chat {
	lang := "en-US,en;q=0.9"
	userAgent := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
	return Chat{
		cookie,
		"",
		"custom",
		model,
		proxies,
		false,
		nil,
		userAgent,
		lang,
	}
}

func (c *Chat) Client(session *emit.Session) {
	c.session = session
}

func (c *Chat) CloudFlare(cookie, userAgent, lang string) {
	c.clearance = cookie
	c.userAgent = userAgent
	if lang != "" {
		c.lang = lang
	}
}

func (c *Chat) Reply(ctx context.Context, chats []Message, fileMessages, query string) (chan string, error) {
	if c.limitWithE {
		count, err := c.State(ctx)
		if err != nil {
			return nil, err
		}
		if count <= 0 {
			return nil, errors.New("ZERO QUOTA")
		}
	}

	messages, err := MergeMessages(chats, false)
	if err != nil {
		return nil, err
	}

	var (
		userFiles = "_"
		files     = ""
	)

	if size := len(fileMessages); size > 0 {
		uf := hex(12)
		filename, e := c.upload(ctx, c.proxies, uf, fileMessages)
		if e != nil {
			return nil, e
		}
		userFiles = "userFiles"
		files = fmt.Sprintf(`[{"user_filename":"%s.txt","filename":"%s","size":"%d"}]`, uf, filename, size)
		if query == "" {
			query = "Please review the attached file: " + uf
		} else {
			query = strings.Replace(query, "{{filename}}", uf, -1)
		}
	}

	chatId := uuid.NewString()
	conversationTurnId := uuid.NewString()
	t := time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
	response, err := emit.ClientBuilder(c.session).
		GET("https://you.com/api/streamingSearch").
		Context(ctx).
		Proxies(c.proxies).
		Ja3().
		Query("q", url.QueryEscape(query)).
		Query("page", "1").
		Query("count", "10").
		Query("safeSearch", "Off").
		Query("mkt", "zh-HK").
		Query("domain", "youchat").
		Query("enable_worklow_generation_ux", "false").
		Query("use_personalization_extraction", "false").
		Query("enable_agent_clarification_questions", "false").
		Query("use_nested_youchat_updates", "true").
		//Query("disable_web_results", "false").
		Query("queryTraceId", uuid.NewString()).
		Query("chatId", chatId).
		Query("conversationTurnId", conversationTurnId).
		Query("selectedChatMode", c.mode).
		Query(userFiles, url.QueryEscape(files)).
		Query(or(c.model == "", "_", "selectedAiModel"), c.model).
		Query("traceId", fmt.Sprintf("%s|%s|%s", chatId, conversationTurnId, t)).
		//Query("incognito", "true").
		//Query("responseFilter", "WebPages,TimeZone,Computation,RelatedSearches").
		Query("pastChatLength", strconv.Itoa(len(chats))).
		Query("chat", url.QueryEscape(messages)).
		Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("User-Agent", c.userAgent).
		Header("Host", "you.com").
		Header("Origin", "https://you.com").
		Header("Referer", "https://you.com/search?fromSearchBar=true&tbm=youchat&chatMode="+c.mode+"&cid="+chatId).
		Header("Accept-Language", c.lang).
		Header("Accept", "text/event-stream").
		DoS(http.StatusOK)
	if err != nil {
		return nil, err
	}

	ch := make(chan string)
	go c.resolve(ctx, ch, response, chatId)
	return ch, nil
}

type state struct {
	Freemium          map[string]int    `json:"freemium"`
	Subscriptions     []interface{} `json:"subscriptions"`
	Org_subscriptions []interface{} `json:"org_subscriptions"`
}

func (c *Chat) State(ctx context.Context) (int, error) {
	response, err := emit.ClientBuilder(c.session).
		Context(ctx).
		Proxies(c.proxies).
		Ja3().
		GET("https://you.com?chatMode=custom").
		Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("User-Agent", c.userAgent).
		Header("Accept-Language", c.lang).
		Header("Referer", "https://you.com/").
		Header("Origin", "https://you.com").
		DoS(http.StatusOK)
	if err != nil {
		return -1, err
	}

	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return -1, err
	}
	bodyString := string(bodyBytes)
	logrus.Infof("Response Body:\n%s", bodyString)

	// 1. 查找 <script id="__NEXT_DATA__" type="application/json"> 标签
	start := strings.Index(bodyString, "<script id=\"__NEXT_DATA__\" type=\"application/json\">")
	if start == -1 {
		return -1, errors.New("script tag with id __NEXT_DATA__ not found")
	}
	start += len("<script id=\"__NEXT_DATA__\" type=\"application/json\">")

	end := strings.Index(bodyString[start:], "</script>")
	if end == -1 {
		return -1, errors.New("closing script tag not found")
	}

	// 2. 提取 JSON 字符串
	jsonStr := bodyString[start : start+end]

	// 3. 直接查找 "youProState" 对应的 JSON 对象
	youProStateStart := strings.Index(jsonStr, `"youProState":{`)
	if youProStateStart == -1 {
		return -1, errors.New(`"youProState" not found in JSON`)
	}
	youProStateStart += len(`"youProState":`)

	// 4. 手动解析 "youProState" 对象
	bracketCount := 0
	youProStateEnd := -1
	for i := youProStateStart; i < len(jsonStr); i++ {
		if jsonStr[i] == '{' {
			bracketCount++
		} else if jsonStr[i] == '}' {
			bracketCount--
		}
		if bracketCount == 0 {
			youProStateEnd = i + 1
			break
		}
	}

	if youProStateEnd == -1 {
		return -1, errors.New(`could not find end of "youProState" object`)
	}

	youProStateStr := jsonStr[youProStateStart:youProStateEnd]

	// 5. 解析 "freemium" 对象
	freemiumStart := strings.Index(youProStateStr, `"freemium":{`)
	if freemiumStart == -1 {
		return -1, errors.New(`"freemium" not found in "youProState"`)
	}
	freemiumStart += len(`"freemium":`)

	bracketCount = 0
	freemiumEnd := -1
	for i := freemiumStart; i < len(youProStateStr); i++ {
		if youProStateStr[i] == '{' {
			bracketCount++
		} else if youProStateStr[i] == '}' {
			bracketCount--
		}
		if bracketCount == 0 {
			freemiumEnd = i + 1
			break
		}
	}

	if freemiumEnd == -1 {
		return -1, errors.New(`could not find end of "freemium" object`)
	}

	freemiumStr := youProStateStr[freemiumStart:freemiumEnd]

	// 6. 提取 "max_calls" 和 "used_calls"
	maxCallsStart := strings.Index(freemiumStr, `"max_calls":`)
	if maxCallsStart == -1 {
		return -1, errors.New(`"max_calls" not found in "freemium"`)
	}
	maxCallsStart += len(`"max_calls":`)
	maxCallsEnd := strings.Index(freemiumStr[maxCallsStart:], ",")
	if maxCallsEnd == -1 {
		maxCallsEnd = len(freemiumStr) - 1 // 到 "}" 前面
	} else {
		maxCallsEnd += maxCallsStart
	}

	maxCallsStr := freemiumStr[maxCallsStart:maxCallsEnd]
	maxCalls, err := strconv.Atoi(strings.TrimSpace(maxCallsStr))
	if err != nil {
		return -1, fmt.Errorf(`error parsing "max_calls": %v`, err)
	}

	usedCallsStart := strings.Index(freemiumStr, `"used_calls":`)
	if usedCallsStart == -1 {
		return -1, errors.New(`"used_calls" not found in "freemium"`)
	}
	usedCallsStart += len(`"used_calls":`)
	usedCallsEnd := strings.Index(freemiumStr[usedCallsStart:], ",")
	if usedCallsEnd == -1 {
		usedCallsEnd = len(freemiumStr) - 1 // 到 "}" 前面
	} else {
		usedCallsEnd += usedCallsStart
	}

	usedCallsStr := freemiumStr[usedCallsStart:usedCallsEnd]
	usedCalls, err := strconv.Atoi(strings.TrimSpace(usedCallsStr))
	if err != nil {
		return -1, fmt.Errorf(`error parsing "used_calls": %v`, err)
	}

	logrus.Infof("used: %d/%d", usedCalls, maxCalls)
	return maxCalls - usedCalls, nil
}

// 创建一个自定义模型，已存在则删除后创建
func (c *Chat) Custom(ctx context.Context, modelName, system string, isNew bool) (err error) {
	response, err := emit.ClientBuilder(c.session).
		Context(ctx).
		Proxies(c.proxies).
		Ja3().
		GET("https://you.com/api/custom_assistants/assistants").
		// Query("filter_type", "all").
		Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("User-Agent", c.userAgent).
		Header("Accept-Language", c.lang).
		Header("Referer", "https://you.com/").
		Header("Origin", "https://you.com").
		DoC(emit.Status(http.StatusOK), emit.IsJSON)
	if err != nil {
		return err
	}

	defer response.Body.Close()
	obj, err := emit.ToMap(response)
	if err != nil {
		return err
	}

	modelId := ""
	models, ok := obj["user_chat_modes"].([]interface{})
	if ok {
		for _, model := range models {
			if info, o := model.(map[string]interface{}); o {
				if info["chat_mode_name"] == modelName {
					modelId = info["chat_mode_id"].(string)
					break
				}
			}
		}
	}

	if modelId != "" {
		if !isNew {
			c.model = ""
			c.mode = modelId
			return
		}

		// 删除自定义模型
		logrus.Infof("delete model: %s", modelName)
		response, err = emit.ClientBuilder(c.session).
			Context(ctx).
			Proxies(c.proxies).
			Ja3().
			DELETE("https://you.com/api/custom_assistants/assistants").
			Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
			Header("User-Agent", c.userAgent).
			Header("Accept-Language", c.lang).
			Header("Referer", "https://you.com/").
			Header("Origin", "https://you.com").
			JSONHeader().
			Body(map[string]interface{}{
				"chatModeId": modelId,
			}).
			DoC(emit.Status(http.StatusOK), emit.IsJSON)
		if err != nil {
			return err
		}
		logrus.Info(emit.TextResponse(response))
		_ = response.Body.Close()
	}

	// 新建自定义模型
	response, err = emit.ClientBuilder(c.session).
		Context(ctx).
		Proxies(c.proxies).
		Ja3().
		POST("https://you.com/api/custom_assistants/assistants").
		Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("User-Agent", c.userAgent).
		Header("Accept-Language", c.lang).
		Header("Referer", "https://you.com/").
		Header("Origin", "https://you.com").
		JSONHeader().
		Body(map[string]interface{}{
			"aiModel":               c.model,
			"name":                  modelName,
			"instructions":          system,
			"instructionsSummary":   "",
			"isUserOwned":           true,
			"visibility":            "private",
			"hideInstructions":      false,
			"hasLiveWebAccess":      false,
			"hasPersonalization":    false,
			"includeFollowUps":      false,
			"advancedReasoningMode": "off",
			"sources":               make([]string, 0),
			"webAccessConfig":       make(map[string]interface{}),
		}).
		DoC(emit.Status(http.StatusOK), emit.IsJSON)
	if err != nil {
		return err
	}

	obj, err = emit.ToMap(response)
	if err != nil {
		return err
	}

	_ = response.Body.Close()
	c.mode = obj["chat_mode_id"].(string)
	c.model = ""
	return
}

// 额度用完是否返回错误
func (c *Chat) LimitWithE(limitWithE bool) {
	c.limitWithE = limitWithE
}

func (c *Chat) delete(chatId string) {
	response, err := emit.ClientBuilder(c.session).
		Proxies(c.proxies).
		Ja3().
		DELETE("https://you.com/api/chat/deleteChat").
		Header("cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("Accept", "application/json, text/plain, */*").
		Header("Accept-Language", c.lang).
		Header("Referer", "https://you.com/?chatMode="+c.mode).
		Header("Origin", "https://you.com").
		Header("User-Agent", c.userAgent).
		JSONHeader().
		Body(map[string]interface{}{
			"chatId": chatId,
		}).DoC(emit.Status(http.StatusOK), emit.IsJSON)
	if err != nil {
		logrus.Error(err)
		return
	}
	defer response.Body.Close()
	logrus.Infof("deleted: %s", emit.TextResponse(response))
}

// 附件上传
func (c *Chat) upload(ctx context.Context, proxies, filename, content string) (string, error) {
	response, err := emit.ClientBuilder(c.session).
		Context(ctx).
		Proxies(proxies).
		Ja3().
		GET("https://you.com/api/get_nonce").
		Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("Accept", "application/json, text/plain, */*").
		Header("Accept-Language", c.lang).
		Header("Referer", "https://you.com/?chatMode="+c.mode).
		Header("Origin", "https://you.com").
		Header("User-Agent", c.userAgent).
		DoS(http.StatusOK)
	if err != nil {
		return "", err
	}

	nonce := emit.TextResponse(response)
	_ = response.Body.Close()

	// doc := docx.NewFile()
	// para := doc.AddParagraph()
	// para.AddText(content)

	var buffer bytes.Buffer

	// h := make(textproto.MIMEHeader)
	// h.Set("Content-Disposition", `form-data; name="file"; filename="messages.docx"`)
	// h.Set("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
	// h.Set("Content-Type", "text/plain")
	// fw, _ := w.CreatePart(h)
	// err = doc.Write(fw)
	// if err != nil {
	//	return "", err
	// }

	w := multipart.NewWriter(&buffer)
	fw, _ := w.CreateFormFile("file", filename+".txt")
	_, err = io.Copy(fw, strings.NewReader(content))
	if err != nil {
		return "", err
	}
	_ = w.Close()

	response, err = emit.ClientBuilder(c.session).
		Context(ctx).
		Proxies(proxies).
		Ja3().
		POST("https://you.com/api/upload").
		Header("X-Upload-Nonce", nonce).
		Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
		Header("Content-Type", w.FormDataContentType()).
		Header("Origin", "https://you.com").
		Header("Accept-Language", c.lang).
		Header("Host", "you.com").
		Header("Accept-Encoding", "br").
		Header("Referer", "https://you.com/?chatMode="+c.mode).
		Header("Origin", "https://you.com").
		Header("Accept", "multipart/form-data").
		Header("User-Agent", c.userAgent).
		Buffer(&buffer).
		DoS(http.StatusOK)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()
	var obj map[string]string
	if err = emit.ToObject(response, &obj); err != nil {
		return "", err
	}

	if fn, ok := obj["filename"]; ok {
		response, err = emit.ClientBuilder(c.session).
			Context(ctx).
			Proxies(proxies).
			Ja3().
			POST("https://you.com/api/instrumentation").
			JSONHeader().
			Header("Cookie", emit.MergeCookies(c.cookie, c.clearance)).
			Header("Origin", "https://you.com").
			Header("Accept-Language", c.lang).
			Header("Host", "you.com").
			Header("Accept-Encoding", "br").
			Header("Referer", "https://you.com/?chatMode="+c.mode).
			Header("Origin", "https://you.com").
			Header("Accept", "application/json, text/plain, */*").
			Header("User-Agent", c.userAgent).
			Bytes([]byte(`{"metricName":"file_upload_client_info_file_drop","documentVisibilityState":"visible","metricType":"info","value":1}`)).
			DoS(http.StatusOK)
		if response != nil {
			_ = response.Body.Close()
		}
		return fn, nil
	}

	return "", errors.New("upload failed")
}

func (c *Chat) resolve(ctx context.Context, ch chan string, response *http.Response, chatId string) {
	defer close(ch)
	defer response.Body.Close()
	defer c.delete(chatId)

	scanner := bufio.NewScanner(response.Body)
	scanner.Split(func(data []byte, eof bool) (advance int, token []byte, err error) {
		if eof && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			return i + 1, data[0:i], nil
		}
		if eof {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	type chatToken struct {
		YouChatToken string `json:"youChatToken"`
	}

	// true 继续，false 结束
	eventHandler := func() bool {
		if !scanner.Scan() {
			return false
		}

		var event string
		data := scanner.Text()
		logrus.Trace("--------- ORIGINAL MESSAGE ---------")
		logrus.Trace(data)
		if data == "" {
			return true
		}

		if len(data) < 7 || data[:7] != "event: " {
			return true
		}
		event = data[7:]

		if event == "done" {
			return false
		}

		if !scanner.Scan() {
			return false
		}

		data = scanner.Text()
		logrus.Trace("--------- ORIGINAL MESSAGE ---------")
		logrus.Trace(data)
		if len(data) < 6 || data[:6] != "data: " {
			return true
		}
		data = data[6:]
		logrus.Trace(data)
		if event == "youChatModeLimits" {
			ch <- "limits: " + data
			return true
		}

		if event != "youChatToken" {
			return true
		}

		var token chatToken
		if err := json.Unmarshal([]byte(data), &token); err != nil {
			return true
		}

		if quotaEmpty(token.YouChatToken) {
			return true
		}

		ch <- token.YouChatToken
		return true
	}

	for {
		select {
		case <-ctx.Done():
			ch <- "error: context canceled"
			return
		default:
			if !eventHandler() {
				return
			}
		}
	}
}

func MergeMessages(messages []Message, files bool) (string, error) {
	if len(messages) == 0 {
		return "[]", nil
	}

	if files {
		var buffer bytes.Buffer
		messageL := len(messages)
		for pos, message := range messages {
			buffer.WriteString(fmt.Sprintf("%s\n\n%s", message.Question, message.Answer))
			if pos < messageL-1 {
				buffer.WriteString("\n\n")
			}
		}
		return buffer.String(), nil
	}

	messageBytes, err := json.Marshal(messages)
	if err != nil {
		return "", err
	}

	return string(messageBytes), nil
}

func quotaEmpty(value string) bool {
	return strings.HasPrefix(value, "#### Please log in to access GPT-4 mode.") ||
		strings.HasPrefix(value, "#### You've hit your free quota for GPT-4 mode.")
}

func extCookies(cookies, model string) (jar http.CookieJar) {
	jar, _ = cookiejar.New(nil)
	u, _ := url.Parse("https://you.com")

	slice := strings.Split(cookies, "; ")
	for _, cookie := range slice {
		kv := strings.Split(cookie, "=")
		if len(kv) < 1 {
			continue
		}

		k := strings.TrimSpace(kv[0])
		v := strings.Join(kv[1:], "=")

		if strings.HasPrefix(k, "safesearch") {
			jar.SetCookies(u, []*http.Cookie{{Name: k, Value: "Off"}})
			continue
		}

		if k == "you_subscription" {
			jar.SetCookies(u, []*http.Cookie{{Name: k, Value: "freemium"}})
			continue
		}

		if k == "ai_model" {
			jar.SetCookies(u, []*http.Cookie{{Name: k, Value: model}})
			continue
		}

		jar.SetCookies(u, []*http.Cookie{{Name: k, Value: strings.TrimSpace(v)}})
	}

	//
	jar.SetCookies(u, []*http.Cookie{{Name: "has_seen_agent_uploads_modal", Value: "true"}})
	return
}

func or[T any](condition bool, v1, v2 T) T {
	if condition {
		return v1
	} else {
		return v2
	}
}

func hex(size int) string {
	bin := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890+_-="
	binL := len(bin)
	var buf bytes.Buffer
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for x := 0; x < size; x++ {
		ch := bin[r.Intn(binL-1)]
		buf.WriteByte(ch)
	}

	return buf.String()
}
