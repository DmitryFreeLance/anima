package ru.animabot;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Модель ключевого слова.
 *  - keyword     : ключ (например "СВОБОДА") — в БД храним в UPPER CASE
 *  - introText   : текст до проверки подписки (с кнопками)
 *  - rewardText  : текст после подтверждения (перед материалами)
 *  - materials   : список материалов (URL, file_id, либо локальный путь)
 */
public class Keyword {
    private String keyword;
    private String introText;
    private String rewardText;
    private List<String> materials = new ArrayList<>();

    public Keyword() {}

    public Keyword(String keyword, String introText, String rewardText, List<String> materials) {
        this.keyword = keyword;
        this.introText = introText;
        this.rewardText = rewardText;
        this.materials = materials != null ? new ArrayList<>(materials) : new ArrayList<>();
    }

    public String getKeyword() { return keyword; }
    public void setKeyword(String keyword) { this.keyword = keyword; }

    public String getIntroText() { return introText; }
    public void setIntroText(String introText) { this.introText = introText; }

    public String getRewardText() { return rewardText; }
    public void setRewardText(String rewardText) { this.rewardText = rewardText; }

    public List<String> getMaterials() { return materials; }
    public void setMaterials(List<String> materials) {
        this.materials = (materials != null) ? new ArrayList<>(materials) : new ArrayList<>();
    }

    /** Удобно, если надо быстро вывести CSV в лог. */
    public String materialsAsString() {
        return materials.stream()
                .map(s -> s == null ? "" : s.trim())
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(","));
    }
}
